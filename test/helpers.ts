/** 测试公共设施：临时配置、可编程假 fetch、SSE 响应构造器。 */
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ConfigStore } from '../src/config/store.js';
import { Logger } from '../src/core/logger.js';

/** 测试里不需要日志噪音 */
export const silentLogger = new Logger('error', 'test', {
  out: { write: () => true },
  err: { write: () => true },
});

export function baseConfig(targets?: unknown[]): Record<string, unknown> {
  return {
    listen: { host: '127.0.0.1', port: 0 },
    defaultModel: 'main',
    aliases: {},
    limits: {
      requestTimeoutMs: 2000,
      firstTokenTimeoutMs: 500,
      idleTimeoutMs: 500,
      totalTimeoutMs: 5000,
      commitChars: 8,
      commitDelayMs: 40,
      // 显式写死：默认值改成 instant 之后，旧用例必须继续覆盖 buffered 语义，
      // 否则"透明切换"这组用例会悄悄变成"提交后只能续写"，覆盖点就漂了。
      commitMode: 'buffered',
      emitRouteComments: false,
      maxBodyBytes: 1048576,
    },
    policy: {
      maxAttempts: 4,
      abortOnBadRequest: true,
      salvageOnStreamBreak: true,
      salvageInstruction: '继续写，不要重复已经输出的内容。',
    },
    models: {
      main: {
        strategy: 'priority',
        targets: targets ?? [
          { id: 'a', adapter: 'openai', baseUrl: 'http://a.test/v1', apiKey: 'ka', model: 'model-a' },
          { id: 'b', adapter: 'openai', baseUrl: 'http://b.test/v1', apiKey: 'kb', model: 'model-b' },
        ],
      },
    },
  };
}

export interface TempStore {
  store: ConfigStore;
  path: string;
  dir: string;
  write(cfg: unknown): Promise<void>;
}

export async function makeStore(
  cfg: unknown = baseConfig(),
  env: NodeJS.ProcessEnv = {},
): Promise<TempStore> {
  const dir = await mkdtemp(join(tmpdir(), 'ai-gw-'));
  const path = join(dir, 'gateway.json');
  await writeFile(path, JSON.stringify(cfg, null, 2), 'utf8');
  const store = await ConfigStore.open(path, { ...process.env, ...env });
  return {
    store,
    path,
    dir,
    write: async (next: unknown) => {
      await writeFile(path, JSON.stringify(next, null, 2), 'utf8');
    },
  };
}

// ── SSE 响应构造 ────────────────────────────────────────────────────────────

const enc = new TextEncoder();

export function sse(chunks: string[], status = 200, headers: Record<string, string> = {}): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(enc.encode(chunk));
      controller.close();
    },
  });
  return new Response(body, { status, headers: { 'content-type': 'text/event-stream', ...headers } });
}

/** 模拟"上游 200 之后 socket 被掐断"：先吐完 chunks，再让流报错。
 *
 *  注意必须是 pull 驱动：如果在 start() 里 enqueue 完立刻 error()，
 *  规范要求丢弃队列里的数据，消费端一个字节都拿不到，就测不出"半截之后断流"。 */
export function brokenSse(chunks: string[], error = new Error('socket hang up')): Response {
  const queue = [...chunks];
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      const next = queue.shift();
      if (next !== undefined) controller.enqueue(enc.encode(next));
      else controller.error(error);
    },
  });
  return new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

export function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });
}

/** 按 gapMs 逐帧吐出的 SSE Response，用来模拟"健康但慢"的上游。
 *
 *  和 sse() 的区别：sse() 一次性 enqueue 完（等价于上游零延迟），
 *  这个用 pull 驱动 + 定时器，帧与帧之间真的隔着 gapMs 墙钟时间。 */
export function pacedSse(chunks: string[], gapMs: number, status = 200): Response {
  const queue = [...chunks];
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      const next = queue.shift();
      if (next === undefined) {
        controller.close();
        return;
      }
      if (gapMs > 0) await new Promise<void>((resolve) => setTimeout(resolve, gapMs));
      controller.enqueue(enc.encode(next));
      if (queue.length === 0) controller.close();
    },
  });
  return new Response(body, { status, headers: { 'content-type': 'text/event-stream' } });
}

export function chunk(payload: Record<string, unknown>): string {
  return `data: ${JSON.stringify(payload)}\n\n`;
}

export const DONE = 'data: [DONE]\n\n';

/** 一段正常的 OpenAI 流式输出 */
export function openAiStream(text: string, id = 'chatcmpl-x'): string[] {
  return [
    chunk({ id, model: 'model-a', choices: [{ index: 0, delta: { role: 'assistant' } }] }),
    ...text.split(/(?<=\s)/).map((piece) => chunk({ id, choices: [{ index: 0, delta: { content: piece } }] })),
    chunk({ id, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }),
    DONE,
  ];
}

// ── 假 fetch ────────────────────────────────────────────────────────────────

export interface RecordedCall {
  url: string;
  body: Record<string, unknown>;
  headers: Record<string, string>;
  /** 上游连接的 signal：用来断言"健康但慢"的流没有被网关自己 abort 掉 */
  signal?: AbortSignal;
}

export interface FakeFetch {
  impl: (url: string, init: RequestInit) => Promise<Response>;
  calls: RecordedCall[];
}

/** 按 host 分发；handler 收到解析后的请求体，方便断言"转发时上下文是否完整"。 */
export function fakeFetch(routes: Record<string, (call: RecordedCall) => Response | Promise<Response>>): FakeFetch {
  const calls: RecordedCall[] = [];
  const impl = async (url: string, init: RequestInit): Promise<Response> => {
    const call: RecordedCall = {
      url,
      body: init.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : {},
      headers: (init.headers ?? {}) as Record<string, string>,
      ...(init.signal ? { signal: init.signal } : {}),
    };
    calls.push(call);
    const host = new URL(url).host;
    const handler = routes[host];
    if (!handler) throw new Error(`unexpected request to ${url}`);
    return handler(call);
  };
  return { impl, calls };
}
