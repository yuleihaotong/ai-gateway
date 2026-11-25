/**
 * 流式"节奏"回归：软提交延迟、首字延迟、背压。
 *
 * 这一组用例对应三类真实缺陷：
 *   P0-1 软提交超时把上游连接 abort 掉 → 健康的慢回答被判成断流；
 *   P1-3 commitChars 计数让首字延迟 = commitChars × 上游每字符间隔（伪流式）；
 *   P2-7 SseWriter 无视 write() 的背压回值。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { AdapterRegistry } from '../src/adapters/registry.js';
import { ProviderHealth } from '../src/core/circuit.js';
import { Gateway, type GatewayItem } from '../src/routing/gateway.js';
import type { CanonRequest } from '../src/protocol/types.js';
import { SseWriter } from '../src/protocol/sse.js';
import { baseConfig, chunk, DONE, fakeFetch, makeStore, pacedSse, silentLogger } from './helpers.js';

const req: CanonRequest = { model: 'main', messages: [{ role: 'user', content: 'hi' }], stream: true };

/** 逐字符吐出的 OpenAI SSE 帧（顺序：role → 每字一帧 → finish_reason → [DONE]） */
function perCharFrames(text: string): string[] {
  return [
    chunk({ id: 'p', choices: [{ index: 0, delta: { role: 'assistant' } }] }),
    ...[...text].map((c) => chunk({ id: 'p', choices: [{ index: 0, delta: { content: c } }] })),
    chunk({ id: 'p', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }),
    DONE,
  ];
}

async function collect(gen: AsyncGenerator<GatewayItem>): Promise<GatewayItem[]> {
  const out: GatewayItem[] = [];
  for await (const item of gen) out.push(item);
  return out;
}

function textOf(items: GatewayItem[]): string {
  let text = '';
  for (const item of items) {
    if (item.kind === 'event' && item.event.type === 'text') text += item.event.delta;
  }
  return text;
}

function routeIds(items: GatewayItem[]): string[] {
  return items.flatMap((i) => (i.kind === 'route' ? [i.providerId] : []));
}

function withLimits(overrides: Record<string, unknown>): Record<string, unknown> {
  const cfg = baseConfig();
  Object.assign(cfg['limits'] as Record<string, unknown>, overrides);
  return cfg;
}

test('健康但慢的短回答不触发切换', async () => {
  const cfg = withLimits({ commitMode: 'buffered', commitChars: 64, commitDelayMs: 40 });
  const { store } = await makeStore(cfg);
  try {
    const health = new ProviderHealth();
    const text = '你好啊';
    // 帧间隔 = commitDelayMs * 3：慢，但每一步都在动 —— 这不是断流
    const { impl, calls } = fakeFetch({
      'a.test': () => pacedSse(perCharFrames(text), 120),
      'b.test': () => pacedSse(perCharFrames('备用不该被叫到'), 10),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const items = await collect(gateway.stream(req));

    assert.deepEqual(routeIds(items), ['a'], '慢 ≠ 死，一次都不该换家');
    assert.equal(items.filter((i) => i.kind === 'salvage').length, 0, '不该出现 salvage');
    assert.equal(textOf(items), text, '一个字都不能少');
    assert.equal(calls.length, 1, '备用模型一次都不该被调用（双倍计费就是这个 bug 的代价）');
    assert.equal(calls[0]?.signal?.aborted, false, '网关自己 abort 了健康的上游连接');
    assert.equal(health.isAvailable('a'), true, '健康的上游不该进冷却');
  } finally {
    store.close();
  }
});

test('慢流在 buffered 模式下也不会误判断流', async () => {
  const cfg = withLimits({ commitMode: 'buffered', commitChars: 64, commitDelayMs: 40 });
  const { store } = await makeStore(cfg);
  try {
    const health = new ProviderHealth();
    const text = '五五开黑节快乐';
    const { impl, calls } = fakeFetch({
      'a.test': () => pacedSse(perCharFrames(text), 80),
      'b.test': () => pacedSse(perCharFrames('wrong'), 10),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const items = await collect(gateway.stream(req));
    const last = items.at(-1);

    assert.equal(items.filter((i) => i.kind === 'salvage').length, 0);
    assert.equal(textOf(items), text);
    assert.equal(last?.kind === 'event' ? last.event.type : 'missing', 'stop', '必须收到终止事件');
    assert.equal(calls.length, 1);
    assert.equal(calls[0]?.signal?.aborted, false);
  } finally {
    store.close();
  }
});

test('instant 模式首字延迟不超过直连 + 60ms', async () => {
  const cfg = withLimits({ commitMode: 'instant', commitChars: 24, commitDelayMs: 400 });
  const { store } = await makeStore(cfg);
  try {
    const health = new ProviderHealth();
    const gapMs = 20;
    const text = 'instant 模式下每一个字都该立刻下发，不该攒到 24 个字才吐第一批。';
    // 直连时首个正文字符大约在第二帧到达时（第一帧是 role chunk）
    const directFirstTextMs = gapMs * 2;
    const { impl } = fakeFetch({ 'a.test': () => pacedSse(perCharFrames(text), gapMs) });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const started = performance.now();
    let firstTextMs: number | undefined;
    const items: GatewayItem[] = [];
    for await (const item of gateway.stream(req)) {
      if (item.kind === 'event' && item.event.type === 'text' && firstTextMs === undefined) {
        firstTextMs = performance.now() - started;
      }
      items.push(item);
    }

    assert.ok(firstTextMs !== undefined, '没有收到任何正文');
    assert.ok(
      firstTextMs <= directFirstTextMs + 60,
      `首字 ${firstTextMs.toFixed(0)}ms 超过直连 + 60ms（直连 ≈ ${directFirstTextMs}ms，阈值 ${directFirstTextMs + 60}ms）`,
    );
    assert.equal(textOf(items), text);
    assert.equal(items.filter((i) => i.kind === 'salvage').length, 0);
  } finally {
    store.close();
  }
});

test('SseWriter 遇 write=false 时等待 drain 且保持帧序', async () => {
  const t0 = Date.now();
  const writes: Array<{ at: number; text: string }> = [];
  let ended = false;
  let drainListener: (() => void) | undefined;

  const sink = {
    write(text: string): boolean {
      writes.push({ at: Date.now() - t0, text });
      // 第一次写就要求背压：后面所有帧都必须等 drain
      if (writes.length === 1) {
        setTimeout(() => drainListener?.(), 80);
        return false;
      }
      return true;
    },
    end(): void {
      ended = true;
    },
    once(_event: 'drain', listener: () => void): void {
      drainListener = listener;
    },
  };

  const writer = new SseWriter(sink);
  writer.comment('a');
  writer.frame('b');
  writer.done();
  await writer.close();

  assert.equal(writes.length, 3, '三帧都写出去了');
  assert.deepEqual(
    writes.map((w) => w.text.startsWith(': ') || w.text.includes('data: ')),
    [true, true, true],
    '帧顺序与内容类型不变',
  );
  assert.ok(writes[0]?.text.includes(': a'), '第一帧是注释行');
  assert.ok(writes[1]?.text.includes('data: b'), '第二帧是原样数据');
  assert.ok(writes[2]?.text.includes('data: [DONE]'), '第三帧是结束哨兵');
  assert.ok((writes[1]?.at ?? 0) >= 70, `背压未生效：第二帧在 ${writes[1]?.at}ms 就写了，说明没等 drain`);
  assert.equal(ended, true, 'close() 必须真的结束响应');
});
