/**
 * 真实 socket 对照测量（不是单测）：慢流误切换、instant 首字延迟、MCP 退出。
 *
 *   node scripts/probe-pacing.mjs
 *
 * 网关是**真的**用 node dist/src/index.js serve 拉起来的子进程，
 * 上游是真的 HTTP 服务，计时用的是墙钟 —— 全程不 mock fetch。
 */
import http from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ENTRY = fileURLToPath(new URL('../dist/src/index.js', import.meta.url));
const GATEWAY_PORT = 18800;
const UPSTREAM_PORT = 18801;
const BACKUP_PORT = 18802;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 一个"健康但慢"的 SSE 上游：每 gapMs 吐一帧，帧内容可配。 */
function pacedUpstream({ chars, gapMs, tag }) {
  const state = { hits: 0, aborted: 0, finished: 0, firstFrameAt: [] };
  const server = http.createServer((req, res) => {
    state.hits += 1;
    const startedAt = Date.now();
    let closed = false;
    res.on('close', () => {
      if (!res.writableEnded) {
        closed = true;
        state.aborted += 1;
      }
    });
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    const frames = [
      `data: ${JSON.stringify({ id: tag, choices: [{ index: 0, delta: { role: 'assistant' } }] })}\n\n`,
      ...[...chars].map(
        (c) => `data: ${JSON.stringify({ id: tag, choices: [{ index: 0, delta: { content: c } }] })}\n\n`,
      ),
      `data: ${JSON.stringify({ id: tag, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`,
      'data: [DONE]\n\n',
    ];
    state.firstFrameAt.push(Date.now() - startedAt);
    void (async () => {
      for (const frame of frames) {
        if (closed) return;
        res.write(frame);
        await sleep(gapMs);
      }
      if (closed) return;
      state.finished += 1;
      res.end();
    })();
  });
  return { server, state };
}

function listen(server, port) {
  return new Promise((resolve) => server.listen(port, '127.0.0.1', resolve));
}

function close(server) {
  return new Promise((resolve) => server.close(() => resolve()));
}

async function writeConfig(path, { commitMode, emitRouteComments }) {
  const cfg = {
    listen: { host: '127.0.0.1', port: GATEWAY_PORT },
    defaultModel: 'smart',
    aliases: {},
    limits: {
      requestTimeoutMs: 60000,
      firstTokenTimeoutMs: 20000,
      idleTimeoutMs: 30000,
      totalTimeoutMs: 300000,
      commitChars: 24,
      commitDelayMs: 400,
      commitMode,
      emitRouteComments,
      maxBodyBytes: 1048576,
    },
    policy: {
      maxAttempts: 4,
      abortOnBadRequest: true,
      salvageOnStreamBreak: true,
      salvageInstruction: '继续写，不要重复已经输出的内容。',
    },
    models: {
      smart: {
        strategy: 'priority',
        targets: [
          { id: 'primary', adapter: 'openai', baseUrl: `http://127.0.0.1:${UPSTREAM_PORT}/v1`, apiKey: 'k1', model: 'primary-model' },
          { id: 'backup', adapter: 'openai', baseUrl: `http://127.0.0.1:${BACKUP_PORT}/v1`, apiKey: 'k2', model: 'backup-model' },
        ],
      },
    },
  };
  await writeFile(path, JSON.stringify(cfg, null, 2), 'utf8');
}

async function startGateway(configPath) {
  const child = spawn(process.execPath, [ENTRY, 'serve'], {
    env: { ...process.env, AI_GATEWAY_CONFIG: configPath, AI_GATEWAY_LOG_LEVEL: 'error' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (c) => (log += c));
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (c) => (log += c));
  for (let i = 0; i < 100; i += 1) {
    try {
      const res = await fetch(`http://127.0.0.1:${GATEWAY_PORT}/healthz`);
      if (res.ok) return { child, log: () => log };
    } catch {
      /* 还没起来 */
    }
    await sleep(100);
  }
  throw new Error(`gateway did not start:\n${log}`);
}

function stop(child) {
  return new Promise((resolve) => {
    child.once('exit', () => resolve());
    child.kill();
  });
}

/** 逐行读 SSE，返回 { comments, text, firstTextMs } */
async function readSse(res) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const startedAt = Date.now();
  let buf = '';
  let firstTextMs = null;
  const comments = [];
  let text = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      for (const line of block.split('\n')) {
        if (line.startsWith(': ')) comments.push(line.slice(2));
        else if (line.startsWith('data: ') && line !== 'data: [DONE]') {
          try {
            const payload = JSON.parse(line.slice(6));
            const delta = payload.choices?.[0]?.delta?.content;
            if (typeof delta === 'string') {
              if (firstTextMs === null) firstTextMs = Date.now() - startedAt;
              text += delta;
            }
          } catch {
            /* 忽略 */
          }
        }
      }
    }
  }
  return { comments, text, firstTextMs };
}

const body = (stream) =>
  JSON.stringify({
    model: 'gpt-4o',
    stream,
    messages: [{ role: 'user', content: 'hi' }],
  });

const results = {};

// ── 相位 1：慢流（3 字 / 600ms 帧间隔）→ 不该换家 ──────────────────────────
{
  const primary = pacedUpstream({ chars: '慢流测', gapMs: 600, tag: 'p' });
  const backup = pacedUpstream({ chars: '备用', gapMs: 10, tag: 'b' });
  await listen(primary.server, UPSTREAM_PORT);
  await listen(backup.server, BACKUP_PORT);

  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-probe-'));
  const cfgPath = join(dir, 'gateway.json');
  await writeConfig(cfgPath, { commitMode: 'buffered', emitRouteComments: true });
  const gw = await startGateway(cfgPath);

  const t0 = Date.now();
  const res = await fetch(`http://127.0.0.1:${GATEWAY_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body(true),
  });
  const out = await readSse(res);

  results.slowStream = {
    totalMs: Date.now() - t0,
    salvages: out.comments.filter((c) => c.startsWith('gateway salvage')).length,
    routes: out.comments.filter((c) => c.startsWith('gateway provider=')).length,
    text: out.text,
    complete: out.text === '慢流测',
    primaryAborted: primary.state.aborted,
    backupHits: backup.state.hits,
    servedBy: res.headers.get('x-gateway-served-by'),
  };

  await stop(gw.child);
  await close(primary.server);
  await close(backup.server);
}

// ── 相位 2：instant 模式首字延迟（直连 vs 过网关）─────────────────────────
{
  const text = 'instant 模式下每一个字都该立刻下发，攒批会让首字变成伪流式。';
  const upstream = pacedUpstream({ chars: text, gapMs: 20, tag: 'p' });
  await listen(upstream.server, UPSTREAM_PORT);
  const backup = pacedUpstream({ chars: 'nope', gapMs: 10, tag: 'b' });
  await listen(backup.server, BACKUP_PORT);

  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-probe-'));
  const cfgPath = join(dir, 'gateway.json');
  await writeConfig(cfgPath, { commitMode: 'instant', emitRouteComments: false });
  const gw = await startGateway(cfgPath);

  const direct = await fetch(`http://127.0.0.1:${UPSTREAM_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body(true),
  });
  const directOut = await readSse(direct);

  const viaGw = await fetch(`http://127.0.0.1:${GATEWAY_PORT}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body(true),
  });
  const gwOut = await readSse(viaGw);

  results.instantLatency = {
    directFirstCharMs: directOut.firstTextMs,
    gatewayFirstCharMs: gwOut.firstTextMs,
    deltaMs: gwOut.firstTextMs - directOut.firstTextMs,
    textComplete: gwOut.text === text,
    comments: gwOut.comments.length,
    servedBy: viaGw.headers.get('x-gateway-served-by'),
  };

  await stop(gw.child);
  await close(upstream.server);
  await close(backup.server);
}

// ── 相位 3：MCP stdin EOF 后是否退出 ────────────────────────────────────
{
  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-mcp-'));
  const cfgPath = join(dir, 'gateway.json');
  await writeConfig(cfgPath, { commitMode: 'instant', emitRouteComments: false });
  // mcp 模式不监听端口，配置里的 listen 不影响
  const child = spawn(process.execPath, [ENTRY, 'mcp'], {
    env: { ...process.env, AI_GATEWAY_CONFIG: cfgPath, AI_GATEWAY_LOG_LEVEL: 'error' },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  const t0 = Date.now();
  child.stdin.write('{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}\n');
  child.stdin.end();
  const code = await new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.kill();
      resolve('TIMEOUT');
    }, 5000);
    child.once('exit', (c) => {
      clearTimeout(timer);
      resolve(c);
    });
  });
  results.mcpExit = { exitCode: code, ms: Date.now() - t0 };
}

console.log(JSON.stringify(results, null, 2));
