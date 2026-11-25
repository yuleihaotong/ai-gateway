import { test } from 'node:test';
import assert from 'node:assert/strict';

import { AdapterRegistry } from '../src/adapters/registry.js';
import { ProviderHealth } from '../src/core/circuit.js';
import { UpstreamFailure } from '../src/core/errors.js';
import { Gateway, type GatewayItem } from '../src/routing/gateway.js';
import type { CanonRequest } from '../src/protocol/types.js';
import {
  baseConfig,
  brokenSse,
  chunk,
  DONE,
  fakeFetch,
  jsonResponse,
  makeStore,
  openAiStream,
  silentLogger,
  sse,
} from './helpers.js';

const req: CanonRequest = { model: 'main', messages: [{ role: 'user', content: 'hi' }], stream: true };
const reqNoStream: CanonRequest = { ...req, stream: false };

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

test('主模型 429 → 客户端只看到备用模型的完整输出（透明切换）', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'Rate limit exceeded' } }, 429, { 'retry-after': '5' }),
      'b.test': () => sse(openAiStream('Hello from b.')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const items = await collect(gateway.stream(req));

    assert.deepEqual(routeIds(items), ['a', 'b']);
    assert.equal(textOf(items), 'Hello from b.');
    const lastItem = items.at(-1);
    assert.equal(lastItem?.kind === 'event' ? lastItem.event.type : 'missing', 'stop');
    // 没有 salvage：客户端一个字节都没收到前就换家了
    assert.equal(items.filter((i) => i.kind === 'salvage').length, 0);
    assert.equal(calls.length, 2);
    assert.equal(health.isAvailable('a'), false, '429 之后主模型进入冷却');
  } finally {
    store.close();
  }
});

test('已提交内容后断流 → 带半截文本去备用模型续写（不重复、不丢字）', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      // 注意：这里既没有 finish_reason 也没有 [DONE]，模拟上游静默截断
      'a.test': () =>
        sse([
          chunk({ id: 'x', choices: [{ index: 0, delta: { role: 'assistant' } }] }),
          chunk({ id: 'x', choices: [{ index: 0, delta: { content: 'Hello ' } }] }),
          chunk({ id: 'x', choices: [{ index: 0, delta: { content: 'there' } }] }),
        ]),
      'b.test': () => sse(openAiStream(' more.', 'chatcmpl-b')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const items = await collect(gateway.stream(req));

    assert.deepEqual(routeIds(items), ['a', 'b']);
    const salvage = items.filter((i) => i.kind === 'salvage');
    assert.equal(salvage.length, 1);
    assert.equal(salvage[0]?.kind === 'salvage' && salvage[0]?.fromProviderId, 'a');
    assert.equal(textOf(items), 'Hello there more.');

    // 续写请求必须把"已经发出去的文字"和续写指令一起带给备用模型
    const forwarded = calls[1]?.body['messages'] as Array<{ role: string; content: string }>;
    assert.equal(forwarded.at(-2)?.role, 'assistant');
    assert.equal(forwarded.at(-2)?.content, 'Hello there');
    assert.equal(forwarded.at(-1)?.role, 'user');
    assert.match(String(forwarded.at(-1)?.content), /继续写/);

    assert.equal(health.state('a').lastReason, 'stream_aborted');
  } finally {
    store.close();
  }
});

test('流异常中断（socket 被掐）同样触发续写', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl } = fakeFetch({
      'a.test': () =>
        brokenSse([
          chunk({ id: 'x', choices: [{ index: 0, delta: { role: 'assistant' } }] }),
          chunk({ id: 'x', choices: [{ index: 0, delta: { content: 'partial answer ' } }] }),
        ]),
      'b.test': () => sse(openAiStream('done.', 'chatcmpl-b')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const items = await collect(gateway.stream(req));
    assert.equal(textOf(items), 'partial answer done.');
    assert.equal(items.filter((i) => i.kind === 'salvage').length, 1);
  } finally {
    store.close();
  }
});

test('400 参数错误不浪费备用额度：直接终止整条链', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'invalid parameter: temperature' } }, 400),
      'b.test': () => sse(openAiStream('should not be reached')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    await assert.rejects(
      () => collect(gateway.stream(req)),
      (err: unknown) => err instanceof UpstreamFailure && err.reason === 'bad_request',
    );
    assert.equal(calls.length, 1);
  } finally {
    store.close();
  }
});

test('非流式：鉴权失败整包换家，返回结果带上实际服务方', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'invalid api key' } }, 401),
      'b.test': () =>
        jsonResponse({
          id: 'chatcmpl-1',
          choices: [{ index: 0, message: { role: 'assistant', content: 'from backup' }, finish_reason: 'stop' }],
          usage: { prompt_tokens: 3, completion_tokens: 4 },
        }),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    const result = await gateway.complete(reqNoStream);
    assert.equal(result.providerId, 'b');
    assert.equal(result.attempts, 2);
    assert.equal(result.response.content, 'from backup');
    assert.equal(result.response.usage?.outputTokens, 4);
  } finally {
    store.close();
  }
});

test('全部候选失败时抛的是最后一个真实错误，而不是笼统的 500', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'rate limit' } }, 429),
      'b.test': () => jsonResponse({ error: { message: 'overloaded' } }, 503),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    await assert.rejects(
      () => gateway.complete(reqNoStream),
      (err: unknown) => err instanceof UpstreamFailure && err.reason === 'upstream_5xx' && err.status === 503,
    );
    assert.equal(calls.length, 2);
  } finally {
    store.close();
  }
});

test('冷却中的主模型在下一个请求里被跳过', async () => {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'quota exceeded' } }, 429),
      'b.test': () => sse(openAiStream('ok')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    await collect(gateway.stream(req));
    const second = await collect(gateway.stream(req));

    assert.deepEqual(routeIds(second), ['b'], '第二次直接打备用');
    assert.equal(calls.filter((c) => c.url.includes('a.test')).length, 1);
  } finally {
    store.close();
  }
});

test('maxAttempts 限制：链上模型再多也只试配置的次数', async () => {
  const cfg = baseConfig();
  (cfg['policy'] as Record<string, unknown>)['maxAttempts'] = 1;
  const { store } = await makeStore(cfg);
  try {
    const health = new ProviderHealth();
    const { impl, calls } = fakeFetch({
      'a.test': () => jsonResponse({ error: { message: 'rate limit' } }, 429),
      'b.test': () => sse(openAiStream('ok')),
    });
    const gateway = new Gateway({
      config: store,
      health,
      adapters: new AdapterRegistry(),
      logger: silentLogger,
      fetchImpl: impl,
    });

    await assert.rejects(() => collect(gateway.stream(req)));
    assert.equal(calls.length, 1);
    void DONE;
  } finally {
    store.close();
  }
});
