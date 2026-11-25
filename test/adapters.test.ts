import { test } from 'node:test';
import assert from 'node:assert/strict';

import { OpenAiCompatibleAdapter } from '../src/adapters/openai.js';
import { AnthropicAdapter } from '../src/adapters/anthropic.js';
import { CodexResponsesAdapter } from '../src/adapters/codex.js';
import { UpstreamFailure } from '../src/core/errors.js';
import type { TargetSpec } from '../src/core/target.js';
import { readSseFrames, type SseFrame } from '../src/protocol/sse.js';
import type { CanonRequest, StreamEvent } from '../src/protocol/types.js';
import { chunk, DONE, openAiStream, sse } from './helpers.js';

const target = (adapters: string, baseUrl = 'http://x.test/v1', model = 'm'): TargetSpec => ({
  id: 'x',
  adapter: adapters,
  baseUrl,
  apiKey: 'sk-test',
  model,
});

const baseReq: CanonRequest = { model: 'main', messages: [{ role: 'user', content: 'hi' }], stream: true };

async function eventsOf(gen: AsyncGenerator<StreamEvent>): Promise<StreamEvent[]> {
  const out: StreamEvent[] = [];
  for await (const ev of gen) out.push(ev);
  return out;
}

function textOf(events: StreamEvent[]): string {
  return events.flatMap((e) => (e.type === 'text' ? [e.delta] : [])).join('');
}

// ── OpenAI 兼容 ─────────────────────────────────────────────────────────────

test('openai 适配器：请求体带上 stream_options 以便拿到 usage', () => {
  const built = new OpenAiCompatibleAdapter().build(target('openai'), baseReq, true);
  const body = JSON.parse(built.body) as Record<string, unknown>;
  assert.equal(built.url, 'http://x.test/v1/chat/completions');
  assert.equal(built.headers['authorization'], 'Bearer sk-test');
  assert.deepEqual(body['stream_options'], { include_usage: true });
  assert.equal(body['model'], 'm');
});

test('openai 适配器：正常流 → start/text/stop', async () => {
  const res = sse(openAiStream('Hello world.', 'id-1'));
  const events = await eventsOf(
    new OpenAiCompatibleAdapter().decodeStream(target('openai'), readSseFrames(res.body!)),
  );
  assert.equal(events[0]?.type, 'start');
  assert.equal(textOf(events), 'Hello world.');
  assert.equal(events.at(-1)?.type, 'stop');
});

test('★ openai 适配器：没有 finish_reason / [DONE] 就 EOF ⇒ 必须抛 stream_aborted', async () => {
  const res = sse([
    chunk({ id: 'x', choices: [{ index: 0, delta: { role: 'assistant' } }] }),
    chunk({ id: 'x', choices: [{ index: 0, delta: { content: 'half an answer' } }] }),
  ]);
  await assert.rejects(
    () => eventsOf(new OpenAiCompatibleAdapter().decodeStream(target('openai'), readSseFrames(res.body!))),
    (err: unknown) => err instanceof UpstreamFailure && err.reason === 'stream_aborted' && err.phase === 'stream',
  );
});

test('openai 适配器：[DONE] 结尾不抛错', async () => {
  const res = sse([chunk({ id: 'x', choices: [{ index: 0, delta: { content: 'ok' } }] }), DONE]);
  const events = await eventsOf(
    new OpenAiCompatibleAdapter().decodeStream(target('openai'), readSseFrames(res.body!)),
  );
  assert.equal(textOf(events), 'ok');
});

test('openai 适配器：流内 error 事件被翻译成失败', async () => {
  const res = sse([chunk({ error: { message: 'current quota exceeded', type: 'insufficient_quota' } })]);
  await assert.rejects(
    () => eventsOf(new OpenAiCompatibleAdapter().decodeStream(target('openai'), readSseFrames(res.body!))),
    (err: unknown) => err instanceof UpstreamFailure && err.reason === 'quota_exhausted',
  );
});

// ── Anthropic ──────────────────────────────────────────────────────────────

test('anthropic 适配器：system 提升为顶层，连续同角色合并', () => {
  const built = new AnthropicAdapter().build(
    target('anthropic'),
    {
      model: 'main',
      stream: true,
      messages: [
        { role: 'system', content: 'be terse' },
        { role: 'user', content: 'a' },
        { role: 'user', content: 'b' },
      ],
    },
    true,
  );
  const body = JSON.parse(built.body) as {
    system?: string;
    messages: Array<{ role: string; content: unknown }>;
    max_tokens: number;
  };
  assert.equal(built.url, 'http://x.test/v1/messages');
  assert.equal(built.headers['x-api-key'], 'sk-test');
  assert.equal(built.headers['anthropic-version'], '2023-06-01');
  assert.equal(body.system, 'be terse');
  assert.equal(body.messages.length, 1, '两条 user 合并成一条');
  assert.deepEqual(body.messages[0]?.content, [
    { type: 'text', text: 'a' },
    { type: 'text', text: 'b' },
  ]);
  assert.equal(body.max_tokens, 4096);
});

test('anthropic 适配器：SSE 事件映射成统一事件流', async () => {
  const res = sse([
    chunk({ type: 'message_start', message: { id: 'msg_1', model: 'claude', usage: { input_tokens: 5 } } }),
    chunk({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
    chunk({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Hello ' } }),
    chunk({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Claude' } }),
    chunk({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 2 } }),
    chunk({ type: 'message_stop' }),
  ]);
  const events = await eventsOf(new AnthropicAdapter().decodeStream(target('anthropic'), readSseFrames(res.body!)));
  assert.equal(textOf(events), 'Hello Claude');
  assert.equal(events.at(-1)?.type, 'stop');
});

test('anthropic 适配器：缺 message_stop ⇒ stream_aborted', async () => {
  const res = sse([chunk({ type: 'message_start', message: { id: 'msg_1' } })]);
  await assert.rejects(
    () => eventsOf(new AnthropicAdapter().decodeStream(target('anthropic'), readSseFrames(res.body!))),
    (err: unknown) => err instanceof UpstreamFailure && err.reason === 'stream_aborted',
  );
});

// ── Codex / Responses API ──────────────────────────────────────────────────

test('codex 适配器：system → instructions，input 用 Responses 的 content 结构', () => {
  const built = new CodexResponsesAdapter().build(
    target('codex'),
    { model: 'main', stream: true, maxTokens: 128, messages: [{ role: 'system', content: 'sys' }, { role: 'user', content: 'hi' }] },
    true,
  );
  const body = JSON.parse(built.body) as {
    instructions?: string;
    input: Array<{ role: string; content: Array<{ type: string; text: string }> }>;
    max_output_tokens?: number;
  };
  assert.equal(built.url, 'http://x.test/v1/responses');
  assert.equal(body.instructions, 'sys');
  assert.deepEqual(body.input[0]?.content, [{ type: 'input_text', text: 'hi' }]);
  assert.equal(body.max_output_tokens, 128);
});

test('codex 适配器：response.output_text.delta → text，completed → stop', async () => {
  const frames: SseFrame[] = [
    { data: JSON.stringify({ type: 'response.created', response: { id: 'resp_1' } }) },
    { data: JSON.stringify({ type: 'response.output_text.delta', delta: 'Yo' }) },
    { data: JSON.stringify({ type: 'response.output_text.delta', delta: 'lo' }) },
    { data: JSON.stringify({ type: 'response.completed', response: { id: 'resp_1', status: 'completed' } }) },
  ];
  const res = sse(frames.map((f) => `data: ${f.data}\n\n`));
  const events = await eventsOf(new CodexResponsesAdapter().decodeStream(target('codex'), readSseFrames(res.body!)));
  assert.equal(textOf(events), 'Yolo');
  assert.equal(events.at(-1)?.type, 'stop');
});

test('codex 适配器：没有 completed/failed ⇒ stream_aborted', async () => {
  const res = sse([chunk({ type: 'response.created', response: { id: 'resp_1' } }), chunk({ type: 'response.output_text.delta', delta: 'x' })]);
  await assert.rejects(
    () => eventsOf(new CodexResponsesAdapter().decodeStream(target('codex'), readSseFrames(res.body!))),
    (err: unknown) => err instanceof UpstreamFailure && err.reason === 'stream_aborted',
  );
});
