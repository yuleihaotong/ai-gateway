/**
 * OpenAI Responses API 适配器（Codex 系列走 /responses）。
 *
 * 它跟 Chat Completions 完全不是一套东西：
 *  - 输入是扁平 input 列表 + 顶层 instructions；
 *  - 流事件是 response.output_text.delta / response.completed 这类命名事件；
 *  - 结束标志是 response.completed，没有 [DONE]。
 * 这正好说明为什么适配层必须存在，而不是在路由层写 if/else。
 */
import type { CanonMessage, CanonRequest, CanonResponse, CanonToolCall, StreamEvent } from '../protocol/types.js';
import type { SseFrame } from '../protocol/sse.js';
import type { TargetSpec } from '../core/target.js';
import { UpstreamFailure } from '../core/errors.js';
import { classifyStreamPayload } from '../core/classifier.js';
import { BaseAdapter, joinUrl, type BuiltRequest } from './adapter.js';

interface ResponsesInputItem {
  role?: string;
  content?: Array<{ type: string; text?: string }>;
  type?: string;
  call_id?: string;
  output?: string;
  name?: string;
  arguments?: string;
}

interface ResponsesEvent {
  type?: string;
  delta?: string;
  output_index?: number;
  item?: { type?: string; id?: string; call_id?: string; name?: string; arguments?: string };
  response?: {
    id?: string;
    model?: string;
    status?: string;
    usage?: { input_tokens?: number; output_tokens?: number };
    output?: Array<{ type?: string; content?: Array<{ type?: string; text?: string }>; call_id?: string; name?: string; arguments?: string }>;
  };
  error?: { code?: string; message?: string };
  message?: string;
}

function toInput(messages: CanonMessage[]): ResponsesInputItem[] {
  const out: ResponsesInputItem[] = [];
  for (const m of messages) {
    if (m.role === 'system') continue;
    if (m.role === 'tool') {
      out.push({ type: 'function_call_output', call_id: m.toolCallId ?? 'call_0', output: m.content });
      continue;
    }
    if (m.role === 'assistant' && m.toolCalls && m.toolCalls.length > 0) {
      for (const call of m.toolCalls) {
        out.push({ type: 'function_call', call_id: call.id, name: call.name, arguments: call.arguments });
      }
      if (m.content) {
        out.push({ role: 'assistant', content: [{ type: 'output_text', text: m.content }] });
      }
      continue;
    }
    out.push({ role: m.role, content: [{ type: m.role === 'assistant' ? 'output_text' : 'input_text', text: m.content }] });
  }
  return out;
}

export class CodexResponsesAdapter extends BaseAdapter {
  readonly kind = 'codex';

  build(target: TargetSpec, req: CanonRequest, stream: boolean): BuiltRequest {
    const instructions = req.messages
      .filter((m) => m.role === 'system')
      .map((m) => m.content)
      .filter(Boolean)
      .join('\n\n');

    const body: Record<string, unknown> = {
      model: target.model,
      input: toInput(req.messages),
      stream,
      ...(target.extraBody ?? {}),
    };
    if (instructions) body['instructions'] = instructions;
    if (req.temperature !== undefined) body['temperature'] = req.temperature;
    if (req.maxTokens !== undefined) body['max_output_tokens'] = req.maxTokens;
    if (req.tools && req.tools.length > 0) {
      body['tools'] = req.tools.map((t) => ({
        type: 'function',
        name: t.name,
        ...(t.description ? { description: t.description } : {}),
        parameters: t.parameters,
      }));
    }

    return {
      url: joinUrl(target.baseUrl, 'responses'),
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
    const obj = (json ?? {}) as ResponsesEvent;
    const output = obj.response?.output ?? [];
    const texts: string[] = [];
    const toolCalls: CanonToolCall[] = [];
    for (const item of output) {
      if (item.type === 'message' && Array.isArray(item.content)) {
        for (const c of item.content) if (c.type === 'output_text' && c.text) texts.push(c.text);
      }
      if (item.type === 'function_call' && item.name) {
        toolCalls.push({ id: item.call_id ?? `call_${toolCalls.length}`, name: item.name, arguments: item.arguments ?? '{}' });
      }
    }
    const status = obj.response?.status;
    return {
      id: obj.response?.id ?? `resp_${Date.now()}`,
      model: target.model,
      content: texts.join(''),
      toolCalls,
      finishReason: status === 'incomplete' ? 'length' : toolCalls.length > 0 ? 'tool_calls' : 'stop',
      ...(obj.response?.usage
        ? { usage: { inputTokens: obj.response.usage.input_tokens, outputTokens: obj.response.usage.output_tokens } }
        : {}),
    };
  }

  async *decodeStream(target: TargetSpec, frames: AsyncGenerator<SseFrame>): AsyncGenerator<StreamEvent> {
    let sawTerminal = false;
    let started = false;
    const toolNames = new Map<number, string>();

    try {
      for await (const frame of frames) {
        if (!frame.data) continue;
        let ev: ResponsesEvent;
        try {
          ev = JSON.parse(frame.data) as ResponsesEvent;
        } catch {
          throw new UpstreamFailure({
            providerId: target.id,
            reason: 'stream_aborted',
            message: `invalid SSE json: ${frame.data.slice(0, 200)}`,
            phase: 'parse',
          });
        }
        const type = frame.event ?? ev.type ?? '';

        switch (type) {
          case 'response.created': {
            started = true;
            yield {
              type: 'start',
              id: ev.response?.id ?? `${target.id}-${Date.now()}`,
              model: ev.response?.model ?? target.model,
            };
            break;
          }
          case 'response.output_text.delta': {
            if (ev.delta) yield { type: 'text', delta: ev.delta };
            break;
          }
          case 'response.reasoning_summary_text.delta':
          case 'response.reasoning_text.delta': {
            if (ev.delta) yield { type: 'reasoning', delta: ev.delta };
            break;
          }
          case 'response.output_item.added': {
            const item = ev.item;
            if (item?.type === 'function_call' && item.name) {
              const index = ev.output_index ?? 0;
              toolNames.set(index, item.name);
              yield { type: 'tool_call', index, ...(item.call_id ? { id: item.call_id } : {}), name: item.name };
            }
            break;
          }
          case 'response.function_call_arguments.delta': {
            if (ev.delta !== undefined) {
              yield { type: 'tool_call', index: ev.output_index ?? 0, argumentsDelta: ev.delta };
            }
            break;
          }
          case 'response.completed': {
            sawTerminal = true;
            const usage = ev.response?.usage;
            if (usage) {
              yield { type: 'usage', usage: { inputTokens: usage.input_tokens, outputTokens: usage.output_tokens } };
            }
            yield { type: 'stop', reason: 'stop' };
            break;
          }
          case 'response.incomplete': {
            sawTerminal = true;
            yield { type: 'stop', reason: 'length' };
            break;
          }
          case 'response.failed':
          case 'error': {
            throw classifyStreamPayload(target.id, ev);
          }
          default:
            break;
        }
        if (type === 'response.completed' || type === 'response.incomplete') break;
      }
    } catch (err) {
      throw this.wrapThrown(target, err);
    }

    if (!sawTerminal) {
      throw new UpstreamFailure({
        providerId: target.id,
        reason: 'stream_aborted',
        message: 'Responses stream closed without response.completed',
        phase: 'stream',
      });
    }
    if (!started) {
      throw new UpstreamFailure({ providerId: target.id, reason: 'stream_aborted', message: 'no response.created', phase: 'stream' });
    }
  }
}
