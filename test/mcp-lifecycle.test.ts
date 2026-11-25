/** MCP 生命周期回归：协议版本协商 + stdin EOF 后进程必须退出。 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { AdapterRegistry } from '../src/adapters/registry.js';
import { ProviderHealth } from '../src/core/circuit.js';
import { Gateway } from '../src/routing/gateway.js';
import { LATEST_PROTOCOL, negotiateProtocol, runMcpServer } from '../src/mcp/server.js';
import { baseConfig, makeStore, silentLogger } from './helpers.js';

interface RpcReply {
  jsonrpc?: string;
  id?: number;
  result?: Record<string, unknown>;
  error?: { code: number; message: string };
}

async function runSession(lines: unknown[]): Promise<RpcReply[]> {
  const { store } = await makeStore();
  try {
    const health = new ProviderHealth();
    const adapters = new AdapterRegistry();
    const gateway = new Gateway({ config: store, health, adapters, logger: silentLogger });
    const written: string[] = [];
    async function* input(): AsyncGenerator<string> {
      for (const line of lines) yield `${JSON.stringify(line)}\n`;
    }
    await runMcpServer({
      gateway,
      config: store,
      health,
      out: {
        write: (chunk: string) => {
          written.push(String(chunk));
          return true;
        },
      },
      input: input(),
    });
    return written.map((chunk) => JSON.parse(chunk) as RpcReply);
  } finally {
    store.close();
  }
}

test('initialize 不回显未知 protocolVersion', async () => {
  const unknown = await runSession([
    { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '1999-01-01' } },
  ]);
  const unknownResult = unknown[0]?.result ?? {};
  assert.equal(unknownResult['protocolVersion'], LATEST_PROTOCOL, '未知版本必须回落到自己支持的最新版');
  assert.match(String(unknownResult['instructions'] ?? ''), /2025-06-18/, '要在 instructions 里说清实际支持的版本');

  const known = await runSession([
    { jsonrpc: '2.0', id: 2, method: 'initialize', params: { protocolVersion: '2025-03-26' } },
  ]);
  assert.equal(known[0]?.result?.['protocolVersion'], '2025-03-26', '支持的版本要原样接受');
  assert.equal(known[0]?.result?.['instructions'], undefined, '正常协商不该出现降级提示');

  assert.deepEqual(negotiateProtocol(undefined), { version: LATEST_PROTOCOL, negotiatedDown: true });
});

test('mcp server exits after stdin EOF', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-mcp-'));
  const cfgPath = join(dir, 'gateway.json');
  await writeFile(cfgPath, JSON.stringify(baseConfig(), null, 2), 'utf8');

  // import.meta.url 指向 dist/test/，所以 ../src/index.js 就是编译产物入口
  const entry = fileURLToPath(new URL('../src/index.js', import.meta.url));
  const child = spawn(process.execPath, [entry, 'mcp'], {
    env: { ...process.env, AI_GATEWAY_CONFIG: cfgPath, AI_GATEWAY_LOG_LEVEL: 'error' },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  let stdout = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', () => undefined);

  child.stdin.write(
    `${JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18' } })}\n`,
  );
  child.stdin.end();

  const code = await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill();
      reject(new Error('stdin EOF 后 3 秒内进程仍未退出：有句柄没关（watchFile / stdin）'));
    }, 3000);
    child.once('exit', (exitCode) => {
      clearTimeout(timer);
      resolve(exitCode);
    });
  });

  assert.equal(code, 0, `应干净退出（exit code 0），实际 ${code}`);
  assert.match(stdout, /"protocolVersion":"2025-06-18"/, 'stdout 只走 JSON-RPC，别混日志');
});
