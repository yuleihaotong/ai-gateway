/** HTTP 入口回归：路由注释是否泄露、/admin/reload 是否可被任意人触发。 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';

import { AdapterRegistry } from '../src/adapters/registry.js';
import { ProviderHealth } from '../src/core/circuit.js';
import { Gateway } from '../src/routing/gateway.js';
import { createGatewayServer, authorizeAdmin, type AppContext } from '../src/index.js';
import { baseConfig, fakeFetch, makeStore, openAiStream, sse, silentLogger } from './helpers.js';

interface Harness {
  url: string;
  close(): Promise<void>;
  ctx: AppContext;
}

async function startServer(cfg: unknown): Promise<Harness> {
  const { store } = await makeStore(cfg);
  const health = new ProviderHealth();
  const adapters = new AdapterRegistry();
  const { impl } = fakeFetch({
    'a.test': () => sse(openAiStream('hello world from a')),
    'b.test': () => sse(openAiStream('from b')),
  });
  const gateway = new Gateway({ config: store, health, adapters, logger: silentLogger, fetchImpl: impl });
  const ctx: AppContext = { config: store, health, adapters, gateway, logger: silentLogger };
  const server = createGatewayServer(ctx);
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    ctx,
    close: async () => {
      await new Promise<void>((done) => server.close(() => done()));
      store.close();
    },
  };
}

function chatBody(): string {
  return JSON.stringify({ model: 'main', stream: true, messages: [{ role: 'user', content: 'hi' }] });
}

test('默认不向客户端泄露路由注释行', async () => {
  const quiet = await startServer(baseConfig());
  try {
    const res = await fetch(`${quiet.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: chatBody(),
    });
    const text = await res.text();

    assert.equal(res.status, 200);
    assert.ok(!text.includes(': gateway'), `SSE 里出现了路由注释：\n${text.slice(0, 200)}`);
    // 承诺"无感知"的同时，排障需要的 served-by 必须放在响应头里
    assert.equal(res.headers.get('x-gateway-served-by'), 'a');
    assert.equal(res.headers.get('x-gateway-attempts'), '1');
  } finally {
    await quiet.close();
  }

  const cfg = baseConfig();
  Object.assign(cfg['limits'] as Record<string, unknown>, { emitRouteComments: true });
  const loud = await startServer(cfg);
  try {
    const res = await fetch(`${loud.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: chatBody(),
    });
    const text = await res.text();
    assert.match(text, /^: gateway provider=a model=model-a attempt=1/, '打开开关后注释行应恢复');
  } finally {
    await loud.close();
  }
});

test('admin reload 拒绝无凭证请求', async () => {
  const server = await startServer(baseConfig());
  const previous = process.env['AI_GATEWAY_ADMIN_TOKEN'];
  try {
    process.env['AI_GATEWAY_ADMIN_TOKEN'] = 's3cret-token';

    const anonymous = await fetch(`${server.url}/admin/reload`, { method: 'POST' });
    assert.equal(anonymous.status, 401, '没带 token 必须 401');
    assert.equal(server.ctx.config.configVersion, 0, '鉴权失败绝不能改配置版本');
    assert.equal(anonymous.headers.get('www-authenticate'), 'Bearer realm="gateway-admin"');

    const wrong = await fetch(`${server.url}/admin/reload`, {
      method: 'POST',
      headers: { authorization: 'Bearer nope' },
    });
    assert.equal(wrong.status, 401);
    assert.equal(server.ctx.config.configVersion, 0);

    const ok = await fetch(`${server.url}/admin/reload`, {
      method: 'POST',
      headers: { authorization: 'Bearer s3cret-token' },
    });
    assert.equal(ok.status, 200);
    assert.equal(server.ctx.config.configVersion, 1);

    // 没配 token 时：非回环来源一律 403（自定义头只在回环下才被当作 CSRF 防护）
    const remote = authorizeAdmin({ headers: { 'x-gateway-admin': '1' }, socket: { remoteAddress: '203.0.113.9' } }, {});
    assert.equal(remote.ok, false);
    assert.equal(remote.status, 403);

    // 回环但没带自定义头：同样拒绝（浏览器简单请求发不出这个头）
    const noCsrfHeader = authorizeAdmin({ headers: {}, socket: { remoteAddress: '127.0.0.1' } }, {});
    assert.equal(noCsrfHeader.ok, false);
    assert.equal(noCsrfHeader.status, 403);

    const loopback = authorizeAdmin({ headers: { 'x-gateway-admin': '1' }, socket: { remoteAddress: '127.0.0.1' } }, {});
    assert.equal(loopback.ok, true);
  } finally {
    if (previous === undefined) delete process.env['AI_GATEWAY_ADMIN_TOKEN'];
    else process.env['AI_GATEWAY_ADMIN_TOKEN'] = previous;
    await server.close();
  }
});
