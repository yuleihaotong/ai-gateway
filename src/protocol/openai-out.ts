/**
 * 出口协议：StreamEvent → OpenAI SSE chunk。
 *
 * 关键点：chunk 里的 id 和 model 全部用**客户端请求的别名**，永远不泄露
 * 实际服务的是 deepseek-backup 还是 claude-backup。调用方（含前端 SDK）无感知。
 */
import type { CanonResponse, StreamEvent } from './types.js';

interface ChunkDelta {
  role?: 'assistant';
  content?: string;
  reasoning_content?: string;
  tool_calls?: Array<{
    index: number;
    id?: string;
    type?: 'function';
    function?: { name?: string; arguments?: string };
  }>;
}

export interface OpenAiChunk {
  id: string;
  object: 'chat.completion.chunk';
  created: number;
  model: string;
  choices: Array<{
    index: number;
    delta: ChunkDelta;
    finish_reason: CanonResponse['finishReason'] | null;
  }>;
  usage?: { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number };
}

export class OpenAiChunkEncoder {
  constructor(
    private readonly id: string,
    private readonly model: string,
    private readonly created: number = Math.floor(Date.now() / 1000),
  ) {}

  /** 返回 null 表示这个事件不需要下发（例如已被合并的 start）。 */
  encode(ev: StreamEvent): string | null {
    switch (ev.type) {
      case 'start':
        return JSON.stringify(this.chunk({ role: 'assistant' }, null));
      case 'text':
        return JSON.stringify(this.chunk({ content: ev.delta }, null));
      case 'reasoning':
        return JSON.stringify(this.chunk({ reasoning_content: ev.delta }, null));
      case 'tool_call': {
        const delta: ChunkDelta = {
          tool_calls: [
            {
              index: ev.index,
              ...(ev.id ? { id: ev.id, type: 'function' as const } : {}),
              ...(ev.name || ev.argumentsDelta
                ? { function: { ...(ev.name ? { name: ev.name } : {}), ...(ev.argumentsDelta ? { arguments: ev.argumentsDelta } : {}) } }
                : {}),
            },
          ],
        };
        return JSON.stringify(this.chunk(delta, null));
      }
      case 'usage': {
        const chunk = this.chunk({}, null);
        chunk.usage = {
          ...(ev.usage.inputTokens !== undefined ? { prompt_tokens: ev.usage.inputTokens } : {}),
          ...(ev.usage.outputTokens !== undefined ? { completion_tokens: ev.usage.outputTokens } : {}),
          ...(ev.usage.inputTokens !== undefined || ev.usage.outputTokens !== undefined
            ? { total_tokens: (ev.usage.inputTokens ?? 0) + (ev.usage.outputTokens ?? 0) }
            : {}),
        };
        return JSON.stringify(chunk);
      }
      case 'stop':
        return JSON.stringify(this.chunk({}, ev.reason));
      case 'error':
        // 在 OpenAI 协议里，流内错误有两种惯例；这里用"带 error 字段的 chunk"，
        // 客户端 SDK 会以异常形式抛出，便于上层提示"本次生成中断"。
        return JSON.stringify({ error: { message: ev.message, type: ev.code } });
      default:
        return null;
    }
  }

  private chunk(delta: ChunkDelta, finishReason: CanonResponse['finishReason'] | null): OpenAiChunk {
    return {
      id: this.id,
      object: 'chat.completion.chunk',
      created: this.created,
      model: this.model,
      choices: [{ index: 0, delta, finish_reason: finishReason }],
    };
  }
}

/** 非流式响应体（聚合之后）。 */
export function toOpenAiCompletion(res: CanonResponse, modelAlias: string): Record<string, unknown> {
  const message: Record<string, unknown> = { role: 'assistant', content: res.content };
  if (res.reasoning) message['reasoning_content'] = res.reasoning;
  if (res.toolCalls.length > 0) {
    message['tool_calls'] = res.toolCalls.map((c) => ({
      id: c.id,
      type: 'function',
      function: { name: c.name, arguments: c.arguments },
    }));
    message['content'] = res.content.length > 0 ? res.content : null;
  }
  return {
    id: res.id,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: modelAlias,
    choices: [{ index: 0, message, finish_reason: res.finishReason }],
    usage: {
      prompt_tokens: res.usage?.inputTokens ?? 0,
      completion_tokens: res.usage?.outputTokens ?? 0,
      total_tokens: (res.usage?.inputTokens ?? 0) + (res.usage?.outputTokens ?? 0),
    },
  };
}

export function toOpenAiError(failure: { reason: string; message: string; status?: number }): Record<string, unknown> {
  const status = failure.status ?? 502;
  const type =
    failure.reason === 'rate_limited' || failure.reason === 'quota_exhausted'
      ? 'rate_limit_error'
      : failure.reason === 'auth_failed'
        ? 'authentication_error'
        : failure.reason === 'bad_request'
          ? 'invalid_request_error'
          : 'upstream_error';
  return {
    error: {
      message: `[gateway] all candidates failed (${failure.reason}): ${failure.message}`.slice(0, 2000),
      type,
      code: failure.reason,
      status,
    },
  };
}
