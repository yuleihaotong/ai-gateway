/**
 * OpenAI 兼容适配器：DeepSeek、GLM(智谱)、Kimi、Qwen、vLLM、one-api …
 * 只要请求/响应形状与 /chat/completions 一致，一行配置就能接进来。
 */
import type { CanonMessage, CanonRequest, CanonResponse, CanonToolCall, StreamEvent } from '../protocol/types.js';
import { FINISH_REASON_MAP } from '../protocol/types.js';
import type { SseFrame } from '../protocol/sse.js';
import type { TargetSpec } from '../core/target.js';
import { UpstreamFailure } from '../core/errors.js';
import { classifyStreamPayload } from '../core/classifier.js';
import { BaseAdapter, joinUrl, type BuiltRequest } from './adapter.js';

interface DeltaShape {
  role?: string;
  content?: string | null;
  reasoning_content?: string | null;
  reasoning?: string | null;
  tool_calls?: Array<{
    index?: number;
    id?: string;
    function?: { name?: string; arguments?: string };
  }>;
}

interface ChunkShape {
  id?: string;
  model?: string;
  choices?: Array<{ index?: number; delta?: DeltaShape; message?: DeltaShape; finish_reason?: string | null }>;
  usage?: { prompt_tokens?: number; completion_tokens?: number };
  error?: unknown;
}

export function toWireMessages(messages: CanonMessage[]): unknown[] {
  return messages.map((m) => {
    if (m.role === 'tool') {
      return { role: 'tool', content: m.content, tool_call_id: m.toolCallId ?? 'call_0' };
    }
    if (m.role === 'assistant' && m.toolCalls && m.toolCalls.length > 0) {
      return {
        role: 'assistant',
        content: m.content.length > 0 ? m.content : null,
        tool_calls: m.toolCalls.map((c) => ({ id: c.id, type: 'function', function: { name: c.name, arguments: c.arguments } })),
      };
    }
    if (m.images && m.images.length > 0) {
      return {
        role: m.role,
        content: [
          ...(m.content ? [{ type: 'text', text: m.content }] : []),
          ...m.images.map((img) => ({ type: 'image_url', image_url: { url: img.url } })),
        ],
      };
    }
    return { role: m.role, content: m.content };
  });
}

export function parseToolCallsFromMessage(msg: DeltaShape | undefined): CanonToolCall[] {
  if (!msg?.tool_calls) return [];
  return msg.tool_calls.map((c, i) => ({
    id: c.id ?? `call_${i}`,
    name: c.function?.name ?? '',
    arguments: c.function?.arguments ?? '{}',
  }));
}

export class OpenAiCompatibleAdapter extends BaseAdapter {
  readonly kind = 'openai';

  build(target: TargetSpec, req: CanonRequest, stream: boolean): BuiltRequest {
    const body: Record<string, unknown> = {
      model: target.model,
      messages: toWireMessages(req.messages),
      stream,
      ...(target.extraBody ?? {}),
    };
    if (stream) {
      // 让上游把 usage 放在最后一个 chunk，聚合时才能拿到 token 数
      body['stream_options'] = { include_usage: true };
    }
    if (req.temperature !== undefined) body['temperature'] = req.temperature;
    if (req.topP !== undefined) body['top_p'] = req.topP;
    if (req.maxTokens !== undefined) body['max_tokens'] = req.maxTokens;
    if (req.stop) body['stop'] = req.stop;
    if (req.tools && req.tools.length > 0) {
      body['tools'] = req.tools.map((t) => ({
        type: 'function',
        function: {
          name: t.name,
          ...(t.description ? { description: t.description } : {}),
          parameters: t.parameters,
        },
      }));
      if (req.toolChoice) {
        body['tool_choice'] =
          typeof req.toolChoice === 'string' ? req.toolChoice : { type: 'function', function: { name: req.toolChoice.name } };
      }
    }

    return {
      url: joinUrl(target.baseUrl, 'chat/completions'),
      headers: {
        'content-type': 'application/json',
        accept: stream ? 'text/event-stream' : 'application/json',
        authorization: `Bearer ${target.apiKey}`,
        ...(target.headers ?? {}),
      },
      body: JSON.stringify(body),
    };
  }

  decodeJson(target: TargetSpec, json: unknown): CanonResponse {
    const obj = (json ?? {}) as ChunkShape & { choices?: Array<{ message?: DeltaShape; finish_reason?: string }> };
    const choice = obj.choices?.[0];
    const msg = choice?.message;
    if (!choice || !msg) {
      throw new UpstreamFailure({ providerId: target.id, reason: 'stream_aborted', message: 'response has no choices', phase: 'parse' });
    }
    return {
      id: obj.id ?? `chatcmpl_${Date.now()}`,
      model: target.model,
      content: typeof msg.content === 'string' ? msg.content : '',
      ...(msg.reasoning_content ? { reasoning: msg.reasoning_content } : {}),
      toolCalls: parseToolCallsFromMessage(msg),
      finishReason: FINISH_REASON_MAP[choice.finish_reason ?? 'stop'] ?? 'stop',
      ...(obj.usage
        ? { usage: { inputTokens: obj.usage.prompt_tokens, outputTokens: obj.usage.completion_tokens } }
        : {}),
    };
  }

  async *decodeStream(target: TargetSpec, frames: AsyncGenerator<SseFrame>): AsyncGenerator<StreamEvent> {
    let started = false;
    let sawTerminal = false;
    try {
      for await (const frame of frames) {
        if (frame.data === '[DONE]') {
          sawTerminal = true;
          break;
        }
        if (!frame.data) continue;

        let json: ChunkShape;
        try {
          json = JSON.parse(frame.data) as ChunkShape;
        } catch {
          throw new UpstreamFailure({
            providerId: target.id,
            reason: 'stream_aborted',
            message: `invalid SSE json: ${frame.data.slice(0, 200)}`,
            phase: 'parse',
          });
        }

        if (json.error) throw classifyStreamPayload(target.id, json);

        if (!started) {
          started = true;
          yield { type: 'start', id: json.id ?? `${target.id}-${Date.now()}`, model: target.model };
        }

        const choice = json.choices?.[0];
        const delta = choice?.delta;
        if (delta) {
          const reasoning = delta.reasoning_content ?? delta.reasoning;
          if (typeof reasoning === 'string' && reasoning.length > 0) yield { type: 'reasoning', delta: reasoning };
          if (typeof delta.content === 'string' && delta.content.length > 0) yield { type: 'text', delta: delta.content };
          if (Array.isArray(delta.tool_calls)) {
            for (const [i, call] of delta.tool_calls.entries()) {
              yield {
                type: 'tool_call',
                index: call.index ?? i,
                ...(call.id ? { id: call.id } : {}),
                ...(call.function?.name ? { name: call.function.name } : {}),
                ...(typeof call.function?.arguments === 'string' ? { argumentsDelta: call.function.arguments } : {}),
              };
            }
          }
        }

        if (json.usage) {
          yield {
            type: 'usage',
            usage: { inputTokens: json.usage.prompt_tokens, outputTokens: json.usage.completion_tokens },
          };
        }

        if (choice?.finish_reason) {
          sawTerminal = true;
          yield { type: 'stop', reason: FINISH_REASON_MAP[choice.finish_reason] ?? 'stop' };
        }
      }
    } catch (err) {
      throw this.wrapThrown(target, err);
    }

    // ★ 上游 200 了、但没有任何"结束标记"就 EOF —— 这是最危险的静默截断。
    //   必须抛出来，让路由层决定是"重放给下一家"还是"续写"。
    if (!sawTerminal) {
      throw new UpstreamFailure({
        providerId: target.id,
        reason: 'stream_aborted',
        message: 'SSE closed without finish_reason or [DONE]',
        phase: 'stream',
      });
    }
  }
}
