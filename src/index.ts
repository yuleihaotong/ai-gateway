#!/usr/bin/env node
/**
 * 主程序入口。两种运行形态共用一套内核：
 *   node dist/src/index.js serve   → OpenAI 兼容 HTTP 中间件（默认 127.0.0.1:8787）
 *   node dist/src/index.js mcp     → MCP Server（stdio，供 Claude Desktop / Cursor 拉起）
 *   node dist/src/index.js check   → 只校验配置，CI 里用
 */
import { createServer, type IncomingMessage, type ServerResponse, type Server } from 'node:http';
import { createHash, timingSafeEqual } from 'node:crypto';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { ConfigStore } from './config/store.js';
import { AdapterRegistry } from './adapters/registry.js';
import { ProviderHealth } from './core/circuit.js';
import { Logger, type LogLevel } from './core/logger.js';
import { UpstreamFailure } from './core/errors.js';
import { parseOpenAiRequest, RequestParseError } from './protocol/openai-in.js';
import { OpenAiChunkEncoder, toOpenAiCompletion, toOpenAiError } from './protocol/openai-out.js';
import { SseWriter } from './protocol/sse.js';
import { Gateway } from './routing/gateway.js';
import { resolveModelAlias, resolveChain } from './routing/policy.js';
import { runMcpServer } from './mcp/server.js';

const CLI = process.argv[2] ?? 'serve';
const CONFIG_PATH = resolve(process.env['AI_GATEWAY_CONFIG'] ?? 'config/gateway.json');
const LOG_LEVEL = (process.env['AI_GATEWAY_LOG_LEVEL'] ?? 'info') as LogLevel;

export interface AppContext {
  config: ConfigStore;
  health: ProviderHealth;
  adapters: AdapterRegistry;
  gateway: Gateway;
  logger: Logger;
}

function statusFor(reason: string): number {
  switch (reason) {
    case 'bad_request':
      return 400;
    case 'rate_limited':
    case 'quota_exhausted':
      return 429;
    case 'timeout':
      return 504;
    default:
      return 502;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  if (res.writableEnded) return;
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload).toString(),
    ...extraHeaders,
  });
  res.end(payload);
}

async function readBody(req: IncomingMessage, limit: number): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = chunk as Buffer;
    size += buf.length;
    if (size > limit) throw new RequestParseError(`request body exceeds ${limit} bytes`);
    chunks.push(buf);
  }
  return Buffer.concat(chunks).toString('utf8');
}

function clientError(message: string): Record<string, unknown> {
  return { error: { message, type: 'invalid_request_error', code: 'bad_request', status: 400 } };
}

async function handleChat(req: IncomingMessage, res: ServerResponse, ctx: AppContext): Promise<void> {
  const cfg = ctx.config.get();
  let canon;
  try {
    const raw = JSON.parse(await readBody(req, cfg.limits.maxBodyBytes)) as unknown;
    canon = parseOpenAiRequest(raw);
  } catch (err) {
    const message = err instanceof RequestParseError || err instanceof SyntaxError ? (err as Error).message : String(err);
    sendJson(res, 400, clientError(message));
    return;
  }

  const requestId = `chatcmpl-${Math.random().toString(36).slice(2, 12)}`;
  const ac = new AbortController();
  res.on('close', () => {
    if (!res.writableEnded) ac.abort(new DOMException('client closed connection', 'AbortError'));
  });

  // ── 流式 ───────────────────────────────────────────────────────────────
  if (canon.stream) {
    let servedProvider: string | undefined;
    let servedAttempt = 0;
    let headersSent = false;

    const writer = new SseWriter({
      write: (chunk) => res.write(chunk),
      end: () => res.end(),
      // 交给 SseWriter 真正的背压信号：write() 返回 false 之后要等 drain 才能继续写
      once: (event, listener) => res.once(event, listener),
    });
    const encoder = new OpenAiChunkEncoder(requestId, canon.model);
    const pendingComments: string[] = [];

    const flushHeaders = (): void => {
      if (headersSent || res.writableEnded) return;
      headersSent = true;
      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache, no-transform',
        connection: 'keep-alive',
        // 关掉 nginx 缓冲，否则"逐字输出"会变成"一次性吐完"
        'x-accel-buffering': 'no',
        'x-gateway-config': String(cfg.configVersion),
        // 流式路径也必须暴露实际服务方，否则和非流式行为不一致，排障只能靠猜
        ...(servedProvider
          ? { 'x-gateway-served-by': servedProvider, 'x-gateway-attempts': String(servedAttempt) }
          : {}),
      });
      for (const comment of pendingComments.splice(0)) void writer.comment(comment);
    };

    // ★ 默认不写路由注释：网关的承诺是"调用方以为一直在和同一个模型对话"，
    //   把 provider 名写进流里等于自己拆自己的台。排障时用 limits.emitRouteComments 打开。
    const noteRoute = (line: string): void => {
      if (!cfg.limits.emitRouteComments) return;
      if (headersSent) void writer.comment(line);
      else pendingComments.push(line);
    };

    try {
      for await (const item of ctx.gateway.stream(canon, ac.signal)) {
        if (item.kind === 'route') {
          servedProvider = item.providerId;
          servedAttempt = item.attempt;
          noteRoute(`gateway provider=${item.providerId} model=${item.model} attempt=${item.attempt}`);
          continue;
        }
        if (item.kind === 'salvage') {
          servedProvider = item.toProviderId;
          servedAttempt += 1;
          noteRoute(`gateway salvage ${item.fromProviderId} -> ${item.toProviderId} (${item.reason})`);
          continue;
        }

        // ★ 第一帧真正的内容到位后才发响应头：这样"所有候选都失败"时
        //   还能返回真实的 429 / 502，而不是先 200 再在流里塞个错误。
        flushHeaders();
        const frame = encoder.encode(item.event);
        // await 让上游的读取跟着 socket 的背压走，而不是先把整条流解码进内存
        if (frame !== null) await writer.frame(frame);
      }

      flushHeaders();
      await writer.done();
    } catch (err) {
      const failure =
        err instanceof UpstreamFailure
          ? err
          : new UpstreamFailure({ providerId: 'gateway', reason: 'unknown', message: String(err) });

      ctx.logger.error('chat stream failed', { requestId, ...failure.toJSON() });

      if (!headersSent) {
        sendJson(res, statusFor(failure.reason), toOpenAiError(failure));
        return;
      }
      // 响应头已经发了，只能在流内报错；客户端 SDK 会以异常形式抛出
      await writer.json({ error: { message: failure.message, type: 'upstream_error', code: failure.reason } }, 'error');
      await writer.done();
    } finally {
      await writer.close();
    }
    return;
  }

  // ── 非流式 ─────────────────────────────────────────────────────────────
  try {
    const result = await ctx.gateway.complete(canon, ac.signal);
    sendJson(res, 200, toOpenAiCompletion(result.response, canon.model), {
      'x-gateway-served-by': result.providerId,
      'x-gateway-attempts': String(result.attempts),
      'x-gateway-config': String(result.configVersion),
    });
  } catch (err) {
    const failure =
      err instanceof UpstreamFailure ? err : new UpstreamFailure({ providerId: 'gateway', reason: 'unknown', message: String(err) });
    ctx.logger.error('chat completion failed', { requestId, ...failure.toJSON() });
    sendJson(res, statusFor(failure.reason), toOpenAiError(failure));
  }
}

/** 管理接口鉴权结果。失败只回原因，永不回显期望的 token 形态。 */
export interface AdminAuthResult {
  ok: boolean;
  status: number;
  reason: string;
}

function timingSafeEquals(a: string, b: string): boolean {
  // 先各自 sha256 再定长比较：直接比原文会因为长度不同提前返回，等于泄露 token 长度
  const ha = createHash('sha256').update(a, 'utf8').digest();
  const hb = createHash('sha256').update(b, 'utf8').digest();
  return timingSafeEqual(ha, hb);
}

function isLoopbackAddress(addr: string | undefined): boolean {
  if (!addr) return false;
  return addr === '127.0.0.1' || addr === '::1' || addr === '::ffff:127.0.0.1';
}

/**
 * /admin/reload 的鉴权。
 *   - 配了 AI_GATEWAY_ADMIN_TOKEN：要求 `Authorization: Bearer <token>`（常量时间比较）
 *   - 没配：退化为"仅回环地址 + 必须带自定义头 x-gateway-admin: 1"。
 *
 * 为什么自定义头能挡 CSRF：浏览器发"简单请求"（form / img / 裸 fetch GET）时**无法**附带
 * 自定义头，跨站页面想触发一次 reload 就必须先过 CORS 预检，而预检里没有这个头的许可。
 */
export function authorizeAdmin(
  req: { headers: Record<string, string | string[] | undefined>; socket?: { remoteAddress?: string | undefined } },
  env: NodeJS.ProcessEnv = process.env,
): AdminAuthResult {
  const token = env['AI_GATEWAY_ADMIN_TOKEN'];
  if (token) {
    const header = req.headers['authorization'];
    const value = Array.isArray(header) ? header[0] : header;
    const matched = /^Bearer\s+(.+)$/i.exec(value ?? '');
    if (!matched?.[1] || !timingSafeEquals(matched[1], token)) {
      return { ok: false, status: 401, reason: 'admin token required' };
    }
    return { ok: true, status: 200, reason: 'token' };
  }

  if (!isLoopbackAddress(req.socket?.remoteAddress)) {
    return { ok: false, status: 403, reason: 'admin endpoint is loopback-only without AI_GATEWAY_ADMIN_TOKEN' };
  }
  const csrf = req.headers['x-gateway-admin'];
  if ((Array.isArray(csrf) ? csrf[0] : csrf) !== '1') {
    return { ok: false, status: 403, reason: 'missing x-gateway-admin: 1 header' };
  }
  return { ok: true, status: 200, reason: 'loopback' };
}

/** CORS 白名单：默认空（同源部署根本不需要 ACAO）。别再用 `*`，那等于把浏览器侧的防线送掉。 */
function corsOriginFor(req: IncomingMessage): string | undefined {
  const allowed = (process.env['AI_GATEWAY_CORS_ORIGINS'] ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const origin = req.headers.origin;
  if (!origin || allowed.length === 0) return undefined;
  return allowed.includes(origin) ? origin : undefined;
}

export function createGatewayServer(ctx: AppContext): Server {
  return createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);

      if (req.method === 'OPTIONS') {
        const origin = corsOriginFor(req);
        res.writeHead(204, {
          ...(origin ? { 'access-control-allow-origin': origin, vary: 'origin' } : {}),
          'access-control-allow-headers': 'authorization, content-type, x-gateway-admin',
          'access-control-allow-methods': 'GET, POST, OPTIONS',
        });
        res.end();
        return;
      }

      if (req.method === 'GET' && url.pathname === '/healthz') {
        const cfg = ctx.config.get();
        sendJson(res, 200, {
          ok: ctx.config.lastLoadError === undefined,
          configVersion: ctx.config.configVersion,
          configError: ctx.config.lastLoadError ?? null,
          defaultModel: cfg.defaultModel,
          models: Object.fromEntries(Object.entries(cfg.models).map(([k, v]) => [k, v.targets.map((t) => t.id)])),
          providers: ctx.health.snapshot(),
        });
        return;
      }

      if (req.method === 'GET' && url.pathname === '/v1/models') {
        const cfg = ctx.config.get();
        const names = new Set([...Object.keys(cfg.models), ...Object.keys(cfg.aliases)]);
        sendJson(res, 200, {
          object: 'list',
          data: [...names].map((id) => ({ id, object: 'model', owned_by: 'gateway' })),
        });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/admin/reload') {
        const auth = authorizeAdmin(req);
        if (!auth.ok) {
          sendJson(
            res,
            auth.status,
            { error: { message: auth.reason, type: 'admin_auth_error', status: auth.status } },
            auth.status === 401 ? { 'www-authenticate': 'Bearer realm="gateway-admin"' } : {},
          );
          return;
        }
        const result = await ctx.config.reload();
        sendJson(res, result.ok ? 200 : 400, result);
        return;
      }

      if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
        await handleChat(req, res, ctx);
        return;
      }

      sendJson(res, 404, clientError(`no route for ${req.method ?? 'GET'} ${url.pathname}`));
    })().catch((err) => {
      ctx.logger.error('unhandled request error', { message: (err as Error).message });
      if (!res.headersSent) sendJson(res, 500, clientError('internal gateway error'));
      else res.end();
    });
  });
}

async function main(): Promise<void> {
  const logger = new Logger(LOG_LEVEL, 'gateway', CLI === 'mcp' ? { out: process.stderr, err: process.stderr } : {});

  let config: ConfigStore;
  try {
    config = await ConfigStore.open(CONFIG_PATH);
  } catch (err) {
    process.stderr.write(`[gateway] failed to load config ${CONFIG_PATH}:\n${(err as Error).message}\n`);
    process.exit(1);
  }

  const health = new ProviderHealth();
  const adapters = new AdapterRegistry();
  const gateway = new Gateway({ config, health, adapters, logger });

  if (CLI === 'check') {
    const cfg = config.get();
    process.stdout.write(
      JSON.stringify(
        {
          ok: true,
          path: CONFIG_PATH,
          defaultModel: cfg.defaultModel,
          models: Object.fromEntries(Object.entries(cfg.models).map(([k, v]) => [k, v.targets.map((t) => `${t.id}(${t.adapter})`)])),
          adapters: adapters.kinds(),
        },
        null,
        2,
      ) + '\n',
    );
    config.close();
    return;
  }

  if (CLI === 'mcp') {
    logger.info('mcp server ready', { config: CONFIG_PATH, version: config.configVersion, adapters: adapters.kinds() });

    // 客户端关掉 stdin 就是"会话结束"。必须把 watchFile 停掉：
    // 它是唯一常驻句柄，不关的话每开一次会话就漏一个 node 进程。
    const shutdown = (): void => {
      config.close();
      process.exit(0);
    };
    process.once('SIGTERM', shutdown);
    process.once('SIGINT', shutdown);

    try {
      await runMcpServer({ gateway, config, health });
    } finally {
      config.close();
      // stdin 已 EOF，destroy 掉避免半开句柄把事件循环吊住
      process.stdin.destroy();
    }
    return;
  }

  const ctx: AppContext = { config, health, adapters, gateway, logger };
  const server = createGatewayServer(ctx);

  const cfg = config.get();
  server.listen(cfg.listen.port, cfg.listen.host, () => {
    logger.info('http gateway listening', {
      url: `http://${cfg.listen.host}:${cfg.listen.port}`,
      configVersion: cfg.configVersion,
      models: Object.keys(cfg.models),
    });
  });

  const shutdown = (signal: string): void => {
    logger.info('shutting down', { signal });
    server.close(() => {
      config.close();
      process.exit(0);
    });
    setTimeout(() => process.exit(0), 3000).unref();
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGHUP', () => {
    void config.reload().then((r) => logger.info('reloaded by SIGHUP', { ...r }));
  });

  // 启动时把候选链打一遍，配置写错能立刻在日志里看到
  logger.info('candidate chains', {
    chains: Object.fromEntries(
      Object.keys(config.get().models).map((alias) => [
        alias,
        resolveChain(config.get(), { model: resolveModelAlias(config.get(), alias), messages: [], stream: false }, health).map(
          (t) => t.id,
        ),
      ]),
    ),
  });
}

// 只有被当成可执行入口时才启动。被 import（测试、其他 harness）时保持纯模块，
// 否则一次 import 就会去读默认路径的配置并起 HTTP 服务。
const INVOKED_ONLY = ((): boolean => {
  const invoked = process.argv[1] ? resolve(process.argv[1]) : '';
  let self = '';
  try {
    self = fileURLToPath(import.meta.url);
  } catch {
    return false;
  }
  if (!invoked) return false;
  return process.platform === 'win32' ? invoked.toLowerCase() === self.toLowerCase() : invoked === self;
})();

if (INVOKED_ONLY) void main();
