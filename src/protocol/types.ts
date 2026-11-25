/**
 * 中间表示（IR）。
 *
 * 网关的"协议转换层"就是把 A 家的 wire format 解析成这里的 CanonRequest，
 * 再由目标模型 Adapter 重新编码成 B 家的 wire format。路由层只认 IR，不认厂商。
 */

export type CanonRole = 'system' | 'user' | 'assistant' | 'tool';

export interface CanonToolCall {
  id: string;
  name: string;
  /** 参数是 JSON 字符串，且可能是流式分片拼起来的 */
  arguments: string;
}

export interface CanonImage {
  type: 'image';
  url: string;
}

export interface CanonMessage {
  role: CanonRole;
  content: string;
  /** assistant 专用 */
  toolCalls?: CanonToolCall[];
  /** role=tool 时对应的调用 id */
  toolCallId?: string;
  name?: string;
  /** 多模态扩展位，目前只透传图片 */
  images?: CanonImage[];
}

export interface CanonToolSpec {
  name: string;
  description?: string;
  parameters: unknown;
}

export interface CanonRequest {
  /** 客户端看到的模型名（别名），不是实际上游模型 */
  model: string;
  messages: CanonMessage[];
  stream: boolean;
  temperature?: number;
  topP?: number;
  maxTokens?: number;
  stop?: string[];
  tools?: CanonToolSpec[];
  toolChoice?: 'auto' | 'none' | 'required' | { name: string };
  /** 客户端的原始额外字段，OpenAI 兼容上游可以透传 */
  extra?: Record<string, unknown>;
}

export interface CanonUsage {
  inputTokens?: number;
  outputTokens?: number;
}

export interface CanonResponse {
  id: string;
  model: string;
  content: string;
  reasoning?: string;
  toolCalls: CanonToolCall[];
  finishReason: 'stop' | 'length' | 'tool_calls' | 'content_filter' | 'error';
  usage?: CanonUsage;
}

/** 流式事件的统一形态 —— 三种上游（OpenAI / Anthropic / Responses）都要压到这里。 */
export type StreamEvent =
  | { type: 'start'; id: string; model: string }
  | { type: 'text'; delta: string }
  | { type: 'reasoning'; delta: string }
  | { type: 'tool_call'; index: number; id?: string; name?: string; argumentsDelta?: string }
  | { type: 'usage'; usage: CanonUsage }
  | { type: 'stop'; reason: CanonResponse['finishReason'] }
  | { type: 'error'; code: string; message: string };

export const FINISH_REASON_MAP: Record<string, CanonResponse['finishReason']> = {
  stop: 'stop',
  end_turn: 'stop',
  stop_sequence: 'stop',
  length: 'length',
  max_tokens: 'length',
  tool_calls: 'tool_calls',
  tool_use: 'tool_calls',
  function_call: 'tool_calls',
  content_filter: 'content_filter',
  refusal: 'content_filter',
};
