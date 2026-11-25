/**
 * Anthropic Messages API 适配器（Claude）。
 *
 * 与 OpenAI 的三处结构性差异，全部在这一层被吃掉：
 *  1) system 不是 message，而是顶层字段；
 *  2) max_tokens 必填；
 *  3) 流事件是 event: xxx + 结构化 content_block_delta，没有 [DONE]。
 */
import type { CanonMessage, CanonRequest, CanonResponse, CanonToolCall, StreamEvent } from '../protocol/types.js';
import { FINISH_REASON_MAP } from '../protocol/types.js';
import type { SseFrame } from '../protocol/sse.js';
import type { TargetSpec } from '../core/target.js';
import { UpstreamFailure } from '../core/errors.js';
import { classifyStreamPayload } from '../core/classifier.js';
import { BaseAdapter, joinUrl, type BuiltRequest } from './adapter.js';

interface AnthropicBlock {
  type?: string;
  text?: string;
  id?: string;
  name?: string;
  input?: unknown;
  tool_use_id?: string;
  content?: unknown;
  source?: unknown;
}

interface AnthropicEvent {
  type?: string;
  index?: number;
  message?: { id?: string; model?: string; usage?: { input_tokens?: number; output_tokens?: number } };
  content_block?: AnthropicBlock;
  delta?: { type?: string; text?: string; partial_json?: string; thinking?: string; stop_reason?: string };
  usage?: { input_tokens?: number; output_tokens?: number };
  error?: { type?: string; message?: string };
}

function toAnthropicMessages(messages: CanonMessage[]): unknown[] {
  const out: unknown[] = [];

  for (const m of messages) {
    if (m.role === 'system') continue;

    if (m.role === 'tool') {
      out.push({
        role: 'user',
        content: [{ type: 'tool_result', tool_use_id: m.toolCallId ?? 'call_0', content: m.content }],
      });
      continue;
    }

    if (m.role === 'assistant' && m.toolCalls && m.toolCalls.length > 0) {
      const blocks: unknown[] = [];
      if (m.content) blocks.push({ type: 'text', text: m.content });
      for (const call of m.toolCalls) {
        let input: unknown = {};
        try {
          input = call.arguments ? JSON.parse(call.arguments) : {};
        } catch {
          input = { raw: call.arguments };
        }
        blocks.push({ type: 'tool_use', id: call.id, name: call.name, input });
      }
      out.push({ role: 'assistant', content: blocks });
      continue;
    }

    if (m.images && m.images.length > 0) {
      const blocks: unknown[] = [];
      if (m.content) blocks.push({ type: 'text', text: m.content });
      for (const img of m.images) blocks.push({ type: 'image', source: { type: 'url', url: img.url } });
      out.push({ role: m.role, content: blocks });
      continue;
    }

    out.push({ role: m.role, content: m.content });
  }

  // Anthropic 要求 user/assistant 交替；连续的相同角色合并，避免 400。
  const merged: Array<{ role: string; content: unknown }> = [];
  for (const item of out as Array<{ role: string; content: unknown }>) {
    const last = merged[merged.length - 1];
    if (last && last.role === item.role) {
      const lastBlocks = Array.isArray(last.content) ? last.content : [{ type: 'text', text: String(last.content) }];
      const curBlocks = Array.isArray(item.content) ? item.content : [{ type: 'text', text: String(item.content) }];
      last.content = [...lastBlocks, ...curBlocks];
    } else {
      merged.push({ role: item.role, content: item.content });
    }
  }
  return merged;
}

export class AnthropicAdapter extends BaseAdapter {
  readonly kind = 'anthropic';

  build(target: TargetSpec, req: CanonRequest, stream: boolean): BuiltRequest {
    const system = req.messages
      .filter((m) => m.role === 'system')
      .map((m) => m.content)
      .filter(Boolean)
      .join('\n\n');

    const body: Record<string, unknown> = {
      model: target.model,
      max_tokens: req.maxTokens ?? 4096,
      messages: toAnthropicMessages(req.messages),
      stream,
      ...(target.extraBody ?? {}),
    };
    if (system) body['system'] = system;
    if (req.temperature !== undefined) body['temperature'] = req.temperature;
    if (req.topP !== undefined) body['top_p'] = req.topP;
    if (req.stop) body['stop_sequences'] = req.stop;
    if (req.tools && req.tools.length > 0) {
      body['tools'] = req.tools.map((t) => ({
        name: t.name,
        ...(t.description ? { description: t.description } : {}),
        input_schema: t.parameters,
      }));
      if (req.toolChoice === 'required') body['tool_choice'] = { type: 'any' };
      else if (req.toolChoice === 'none') body['tool_choice'] = { type: 'none' };
      else if (typeof req.toolChoice === 'object') body['tool_choice'] = { type: 'tool', name: req.toolChoice.name };
    }

    return {
      url: joinUrl(target.baseUrl, 'messages'),
      headers: {
        'content-type': 'application/json',
        accept: stream ? 'text/event-stream' : 'application/json',
        'x-api-key': target.apiKey,
        'anthropic-version': target.headers?.['anthropic-version'] ?? '2023-06-01',
        ...(target.headers ?? {}),
      },
      body: JSON.stringify(body),
    };
  }

  decodeJson(target: TargetSpec, json: unknown): CanonResponse {
    const obj = (json ?? {}) as {
      id?: string;
      content?: AnthropicBlock[];
      stop_reason?: string;
      usage?: { input_tokens?: number; output_tokens?: number };
    };
    if (!obj.content) {
      throw new UpstreamFailure({ providerId: target.id, reason: 'stream_aborted', message: 'no content in response', phase: 'parse' });
    }
    const texts: string[] = [];
    const toolCalls: CanonToolCall[] = [];
    for (const block of obj.content) {
      if (block.type === 'text' && typeof block.text === 'string') texts.push(block.text);
      if (block.type === 'tool_use' && block.id && block.name) {
        toolCalls.push({ id: block.id, name: block.name, arguments: JSON.stringify(block.input ?? {}) });
      }
    }
    return {
      id: obj.id ?? `msg_${Date.now()}`,
      model: target.model,
      content: texts.join(''),
      toolCalls,
      finishReason: FINISH_REASON_MAP[obj.stop_reason ?? 'end_turn'] ?? 'stop',
      ...(obj.usage ? { usage: { inputTokens: obj.usage.input_tokens, outputTokens: obj.usage.output_tokens } } : {}),
    };
  }

  async *decodeStream(target: TargetSpec, frames: AsyncGenerator<SseFrame>): AsyncGenerator<StreamEvent> {
    let sawStop = false;
    let stopReason: CanonResponse['finishReason'] = 'stop';
    let started = false;

    try {
      for await (const frame of frames) {
        if (!frame.data) continue;
        let ev: AnthropicEvent;
        try {
          ev = JSON.parse(frame.data) as AnthropicEvent;
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
          case 'message_start': {
            started = true;
            yield { type: 'start', id: ev.message?.id ?? `${target.id}-${Date.now()}`, model: target.model };
            if (ev.message?.usage?.input_tokens !== undefined) {
              yield { type: 'usage', usage: { inputTokens: ev.message.usage.input_tokens } };
            }
            break;
          }
          case 'content_block_start': {
            const block = ev.content_block;
            if (block?.type === 'tool_use' && block.id && block.name) {
              yield { type: 'tool_call', index: ev.index ?? 0, id: block.id, name: block.name };
            }
            break;
          }
          case 'content_block_delta': {
            const d = ev.delta;
            if (d?.type === 'text_delta' && d.text) yield { type: 'text', delta: d.text };
            else if (d?.type === 'thinking_delta' && d.thinking) yield { type: 'reasoning', delta: d.thinking };
            else if (d?.type === 'input_json_delta' && d.partial_json !== undefined) {
              yield { type: 'tool_call', index: ev.index ?? 0, argumentsDelta: d.partial_json };
            }
            break;
          }
          case 'message_delta': {
            if (ev.delta?.stop_reason) stopReason = FINISH_REASON_MAP[ev.delta.stop_reason] ?? 'stop';
            if (ev.usage?.output_tokens !== undefined) {
              yield { type: 'usage', usage: { outputTokens: ev.usage.output_tokens } };
            }
            sawStop = true;
            yield { type: 'stop', reason: stopReason };
            break;
          }
          case 'message_stop': {
            sawStop = true;
            break;
          }
          case 'ping': {
            break;
          }
          case 'error': {
            throw classifyStreamPayload(target.id, ev);
          }
          default:
            break;
        }
        if (type === 'message_stop') break;
      }
    } catch (err) {
      throw this.wrapThrown(target, err);
    }

    if (!sawStop) {
      throw new UpstreamFailure({
        providerId: target.id,
        reason: 'stream_aborted',
        message: 'Anthropic stream closed without message_stop',
        phase: 'stream',
      });
    }
    if (!started) {
      throw new UpstreamFailure({ providerId: target.id, reason: 'stream_aborted', message: 'no message_start', phase: 'stream' });
    }
  }
}
