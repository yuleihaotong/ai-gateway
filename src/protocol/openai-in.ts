/**
 * 入口协议：OpenAI Chat Completions 请求体 → CanonRequest。
 *
 * 网关对外只暴露一种协议（OpenAI 兼容），因为现在几乎所有客户端都认它；
 * 上游可以是任何形状，收敛发生在 Adapter 里。
 */
import type { CanonMessage, CanonRequest, CanonToolCall, CanonToolSpec } from './types.js';

interface WireContentPart {
  type?: string;
  text?: string;
  image_url?: { url?: string } | string;
}

interface WireMessage {
  role?: string;
  content?: string | WireContentPart[] | null;
  tool_calls?: Array<{ id?: string; type?: string; function?: { name?: string; arguments?: string } }>;
  tool_call_id?: string;
  name?: string;
}

export class RequestParseError extends Error {}

function parseContent(raw: WireMessage['content']): { text: string; images: string[] } {
  if (raw == null) return { text: '', images: [] };
  if (typeof raw === 'string') return { text: raw, images: [] };
  const texts: string[] = [];
  const images: string[] = [];
  for (const part of raw) {
    if (typeof part === 'string') {
      texts.push(part);
      continue;
    }
    if (part.type === 'text' && typeof part.text === 'string') texts.push(part.text);
    else if (part.type === 'image_url') {
      const url = typeof part.image_url === 'string' ? part.image_url : part.image_url?.url;
      if (url) images.push(url);
    }
  }
  return { text: texts.join('\n'), images };
}

function parseToolCalls(raw: WireMessage['tool_calls']): CanonToolCall[] {
  if (!raw) return [];
  return raw.map((call, i) => ({
    id: call.id ?? `call_${i}`,
    name: call.function?.name ?? '',
    arguments: call.function?.arguments ?? '{}',
  }));
}

function parseMessages(raw: unknown): CanonMessage[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new RequestParseError('`messages` must be a non-empty array');
  }
  return raw.map((item, idx) => {
    const m = (item ?? {}) as WireMessage;
    const role = m.role;
    if (role !== 'system' && role !== 'user' && role !== 'assistant' && role !== 'tool') {
      throw new RequestParseError(`messages[${idx}].role is invalid: ${String(role)}`);
    }
    const { text, images } = parseContent(m.content);
    const msg: CanonMessage = { role, content: text };
    if (role === 'assistant') {
      const calls = parseToolCalls(m.tool_calls);
      if (calls.length > 0) msg.toolCalls = calls;
    }
    if (role === 'tool' && typeof m.tool_call_id === 'string') msg.toolCallId = m.tool_call_id;
    if (typeof m.name === 'string') msg.name = m.name;
    if (images.length > 0) msg.images = images.map((url) => ({ type: 'image' as const, url }));
    return msg;
  });
}

function parseTools(raw: unknown): CanonToolSpec[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((t) => (t ?? {}) as { type?: string; function?: { name?: string; description?: string; parameters?: unknown } })
    .filter((t) => t.type === 'function' && typeof t.function?.name === 'string')
    .map((t) => ({
      name: t.function!.name as string,
      ...(t.function?.description ? { description: t.function.description } : {}),
      parameters: t.function?.parameters ?? { type: 'object', properties: {} },
    }));
}

export function parseOpenAiRequest(body: unknown): CanonRequest {
  const raw = (body ?? {}) as Record<string, unknown>;
  if (typeof raw['model'] !== 'string' || !raw['model']) {
    throw new RequestParseError('`model` is required');
  }

  const req: CanonRequest = {
    model: raw['model'],
    messages: parseMessages(raw['messages']),
    stream: raw['stream'] === true,
  };

  if (typeof raw['temperature'] === 'number') req.temperature = raw['temperature'];
  if (typeof raw['top_p'] === 'number') req.topP = raw['top_p'];
  if (typeof raw['max_tokens'] === 'number') req.maxTokens = raw['max_tokens'];
  else if (typeof raw['max_completion_tokens'] === 'number') req.maxTokens = raw['max_completion_tokens'];
  if (Array.isArray(raw['stop'])) req.stop = raw['stop'].filter((s): s is string => typeof s === 'string');
  else if (typeof raw['stop'] === 'string') req.stop = [raw['stop']];

  const tools = parseTools(raw['tools']);
  if (tools.length > 0) req.tools = tools;
  const toolChoice = raw['tool_choice'];
  if (toolChoice === 'auto' || toolChoice === 'none' || toolChoice === 'required') req.toolChoice = toolChoice;
  else if (toolChoice && typeof toolChoice === 'object') {
    const name = (toolChoice as { function?: { name?: string } }).function?.name;
    if (name) req.toolChoice = { name };
  }

  const extra: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw)) {
    if (
      !['model', 'messages', 'stream', 'temperature', 'top_p', 'max_tokens', 'max_completion_tokens', 'stop', 'tools', 'tool_choice'].includes(k)
    ) {
      extra[k] = v;
    }
  }
  if (Object.keys(extra).length > 0) req.extra = extra;
  return req;
}
