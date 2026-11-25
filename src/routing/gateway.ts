/**
 * 路由层核心：候选链执行器 + 流式"软提交"降级。
 *
 * ── 为什么流式降级比非流式难一个数量级 ──────────────────────────────
 * 非流式：上游返回 429 → 换一家重发，客户端什么都没感知。
 * 流式：一旦第一个字节写进 socket 并 flush，HTTP 状态码和响应头就焊死了。
 *       此时主模型挂了，你只有两种选择：
 *         (a) 直接掐断，客户端看到半截文本（体验灾难）；
 *         (b) 换一家**从断点续写**（salvage），客户端看到的是连续文本。
 *
 * 所以这里引入"软提交（soft commit）"：
 *   - 拿到第一批 delta 后先不 flush，攒够 commitBytes 或等 commitDelayMs；
 *   - 提交之前失败 → 丢弃缓冲、换下一家，客户端零感知（transparent）；
 *   - 提交之后失败 → salvage 续写；续写也失败才真正报错。
 */
import { ProviderHealth } from '../core/circuit.js';
import { UpstreamFailure } from '../core/errors.js';
import { asFailure } from '../core/classifier.js';
import type { TargetSpec } from '../core/target.js';
import type { Logger } from '../core/logger.js';
import type { CanonRequest, CanonResponse, StreamEvent } from '../protocol/types.js';
import { readSseFrames } from '../protocol/sse.js';
import type { AdapterRegistry } from '../adapters/registry.js';
import type { ConfigStore } from '../config/store.js';
import { resolveChain, withContinuation } from './policy.js';

export type FetchLike = (input: string, init: RequestInit) => Promise<Response>;

export type GatewayItem =
  | { kind: 'route'; providerId: string; model: string; attempt: number }
  | { kind: 'salvage'; fromProviderId: string; toProviderId: string; reason: string }
  | { kind: 'event'; event: StreamEvent };

export interface GatewayDeps {
  config: ConfigStore;
  health: ProviderHealth;
  adapters: AdapterRegistry;
  logger: Logger;
  fetchImpl?: FetchLike;
  now?: () => number;
}

export interface CompleteResult {
  response: CanonResponse;
  providerId: string;
  attempts: number;
  configVersion: number;
}

type NextResult<T> = { kind: 'value'; value: T } | { kind: 'end' } | { kind: 'error'; error: unknown };

/**
 * 竞速等待一个 pending 的 next()：到点就返回 flush。
 *
 * ★ 与普通 withTimeout 的本质区别：超时**不 abort 上游、也不丢弃那个 promise**。
 *   被丢弃的 next() 稍后 resolve 出来的那一帧会永久消失（客户端静默少字），
 *   所以由调用方把 promise 提到循环外复用，这里只负责"先不等了"。
 *
 * 注意 pending 的 rejection 已经由调用方 .then(_, handler) 接住，不会冒 unhandledRejection。
 */
async function raceNext<T>(pending: Promise<NextResult<T>>, ms: number): Promise<NextResult<T> | { kind: 'flush' }> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<{ kind: 'flush' }>((resolve) => {
    timer = setTimeout(() => resolve({ kind: 'flush' }), Math.max(1, ms));
    timer.unref?.();
  });

  try {
    return await Promise.race([pending, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** code point 计数：汉字/emoji 在 UTF-16 里占 2 个 code unit，用 .length 会让中文提前一半提交。 */
function cpLen(text: string): number {
  let n = 0;
  for (const _ of text) n += 1;
  return n;
}

function relayAbort(clientSignal: AbortSignal | undefined, ac: AbortController): () => void {
  if (!clientSignal) return () => {};
  if (clientSignal.aborted) {
    ac.abort(clientSignal.reason);
    return () => {};
  }
  const onAbort = (): void => ac.abort(clientSignal.reason);
  clientSignal.addEventListener('abort', onAbort, { once: true });
  return () => clientSignal.removeEventListener('abort', onAbort);
}

async function safeText(res: Response): Promise<string> {
  try {
    return (await res.text()).slice(0, 4000);
  } catch {
    return '';
  }
}

function textOf(ev: StreamEvent): string {
  return ev.type === 'text' ? ev.delta : '';
}

/** 什么时候可以把缓冲 flush 给客户端（= 放弃"还能悄悄换一家"的权利）。 */
function shouldCommit(ev: StreamEvent, buffer: StreamEvent[], commitChars: number): boolean {
  if (ev.type === 'stop' || ev.type === 'error') return true;
  if (ev.type === 'tool_call' && ev.name) return true;
  if (ev.type === 'text') {
    let len = 0;
    for (const e of buffer) if (e.type === 'text') len += cpLen(e.delta);
    if (len >= commitChars) return true;
  }
  return false;
}

export class Gateway {
  constructor(private readonly deps: GatewayDeps) {}

  private get fetchImpl(): FetchLike {
    return this.deps.fetchImpl ?? ((input, init) => fetch(input, init));
  }

  private get now(): () => number {
    return this.deps.now ?? Date.now;
  }

  /** 非流式：整包 JSON，失败就整包换下一家，没有半截问题。 */
  async complete(req: CanonRequest, clientSignal?: AbortSignal): Promise<CompleteResult> {
    const cfg = this.deps.config.get();
    const chain = resolveChain(cfg, req, this.deps.health);
    const log = this.deps.logger;
    let attempts = 0;
    let last: UpstreamFailure | undefined;

    for (const target of chain) {
      if (attempts >= cfg.policy.maxAttempts) break;
      if (clientSignal?.aborted) break;
      if (!this.deps.health.isAvailable(target.id)) {
        log.debug('skip cooling provider', { provider: target.id });
        continue;
      }
      attempts += 1;

      const ac = new AbortController();
      const detach = relayAbort(clientSignal, ac);
      const timeoutMs = target.timeoutMs ?? cfg.limits.requestTimeoutMs;
      const timer = setTimeout(
        () => ac.abort(new DOMException(`attempt timeout ${timeoutMs}ms`, 'TimeoutError')),
        timeoutMs,
      );
      timer.unref?.();
      const startedAt = this.now();

      try {
        const adapter = this.deps.adapters.require(target.adapter);
        const built = adapter.build(target, req, false);
        const res = await this.fetchImpl(built.url, {
          method: 'POST',
          headers: built.headers,
          body: built.body,
          signal: ac.signal,
        });

        if (!res.ok) {
          const bodyText = await safeText(res);
          throw adapter.decodeError(target, res.status, res.headers, bodyText);
        }

        const json = (await res.json()) as unknown;
        const response = adapter.decodeJson(target, json);
        this.deps.health.recordSuccess(target.id);
        log.info('completion served', {
          provider: target.id,
          attempts,
          ms: this.now() - startedAt,
          configVersion: cfg.configVersion,
        });
        return { response, providerId: target.id, attempts, configVersion: cfg.configVersion };
      } catch (err) {
        const failure = asFailure(err, target.id);
        // 客户端自己断开，不要记在上游账上（否则一次用户取消就能把主模型打进冷却）
        if (clientSignal?.aborted) throw failure;
        this.deps.health.recordFailure(target.id, failure);
        last = failure;
        log.warn('completion attempt failed', { ...failure.toJSON(), attempts });
        if (failure.abortsChain && cfg.policy.abortOnBadRequest) throw failure;
      } finally {
        clearTimeout(timer);
        detach();
      }
    }

    throw last ?? new UpstreamFailure({ providerId: 'none', reason: 'unknown', message: 'no candidate target available' });
  }

  /** 流式：软提交 + 透明切换 + 断点续写。 */
  async *stream(req: CanonRequest, clientSignal?: AbortSignal): AsyncGenerator<GatewayItem> {
    const cfg = this.deps.config.get();
    const chain = resolveChain(cfg, req, this.deps.health);
    const log = this.deps.logger;
    const deadline = this.now() + cfg.limits.totalTimeoutMs;

    let currentReq = req;
    let attempts = 0;
    let committed = false;
    let committedText = '';
    let buffer: StreamEvent[] = [];
    let last: UpstreamFailure | undefined;

    for (const target of chain) {
      if (attempts >= cfg.policy.maxAttempts || this.now() >= deadline || clientSignal?.aborted) break;
      if (!this.deps.health.isAvailable(target.id)) {
        log.debug('skip cooling provider', { provider: target.id });
        continue;
      }
      attempts += 1;
      yield { kind: 'route', providerId: target.id, model: target.model, attempt: attempts };

      let iterator: AsyncGenerator<StreamEvent> | undefined;
      const ac = new AbortController();
      const detach = relayAbort(clientSignal, ac);
      const budget = Math.max(1000, Math.min(target.timeoutMs ?? cfg.limits.requestTimeoutMs, deadline - this.now()));
      const attemptTimer = setTimeout(
        () => ac.abort(new DOMException(`attempt timeout ${budget}ms`, 'TimeoutError')),
        budget,
      );
      attemptTimer.unref?.();

      let sawTerminal = false;
      let dataStartedAt: number | undefined;
      const startedAt = this.now();

      try {
        const adapter = this.deps.adapters.require(target.adapter);
        const built = adapter.build(target, currentReq, true);
        const res = await this.fetchImpl(built.url, {
          method: 'POST',
          headers: built.headers,
          body: built.body,
          signal: ac.signal,
        });

        if (!res.ok) {
          const bodyText = await safeText(res);
          throw adapter.decodeError(target, res.status, res.headers, bodyText);
        }
        if (!res.body) {
          throw new UpstreamFailure({
            providerId: target.id,
            reason: 'stream_aborted',
            message: 'response has no body',
            phase: 'request',
          });
        }

        iterator = adapter.decodeStream(target, readSseFrames(res.body, { signal: ac.signal }));
        const it = iterator;

        // ★ pending 提到循环外复用：软提交延迟只是"先不等了"，不是"这一帧不要了"。
        //   如果每轮都新调 it.next()，被丢弃的旧 promise 稍后 resolve 出的那一帧就永久消失，
        //   表现是客户端静默少字 —— 比直接报错更难查。
        let pending: Promise<NextResult<StreamEvent>> | null = null;
        const peek = (): Promise<NextResult<StreamEvent>> => {
          pending ??= it.next().then(
            (r): NextResult<StreamEvent> => (r.done ? { kind: 'end' } : { kind: 'value', value: r.value }),
            (error): NextResult<StreamEvent> => ({ kind: 'error', error }),
          );
          return pending;
        };
        const take = (r: NextResult<StreamEvent>): NextResult<StreamEvent> => {
          pending = null;
          return r;
        };

        for (;;) {
          // 已提交：只要别长时间不吐字就行；未提交：首字超时 + 软缓冲延迟
          const waitingFirstToken = !committed && dataStartedAt === undefined;
          const waitMs = committed
            ? cfg.limits.idleTimeoutMs
            : waitingFirstToken
              ? cfg.limits.firstTokenTimeoutMs
              : cfg.limits.commitDelayMs;

          const raced = await raceNext(peek(), waitMs);

          if (raced.kind === 'flush') {
            if (!committed && dataStartedAt !== undefined) {
              // ★ 软提交延迟到点：只把已攒的帧发出去，**绝不 abort 上游连接**。
              //   上游只是慢，不是死。在这里 abort 会让下一次 next() 立刻抛错，
              //   于是一个健康的长尾回答被判成断流 → 白白续写一次，双倍计费 + 语义断层。
              committed = true;
              for (const ev of buffer) {
                committedText += textOf(ev);
                yield { kind: 'event', event: ev };
              }
              buffer = [];
              continue;
            }
            // 只有这两种才是"真超时"，该掐连接：首字一个字节都没来；或已提交后长时间不吐字
            ac.abort(new DOMException(`no data for ${waitMs}ms`, 'TimeoutError'));
            throw new UpstreamFailure({
              providerId: target.id,
              reason: 'timeout',
              message: committed
                ? `stream idle for ${cfg.limits.idleTimeoutMs}ms`
                : `first token timeout after ${cfg.limits.firstTokenTimeoutMs}ms`,
              phase: 'stream',
            });
          }

          const next = take(raced);
          if (next.kind === 'end') break;
          if (next.kind === 'error') throw next.error;

          const ev = next.value;
          if (ev.type === 'stop' || ev.type === 'error') sawTerminal = true;
          if (dataStartedAt === undefined && ev.type !== 'start') dataStartedAt = this.now();

          if (committed) {
            // 已经发过 role chunk 了，重复的 start 会污染客户端状态机
            if (ev.type === 'start') continue;
            committedText += textOf(ev);
            yield { kind: 'event', event: ev };
            continue;
          }

          if (cfg.limits.commitMode === 'instant') {
            // instant：一帧都不攒。首字延迟 = 上游首字延迟；
            // 代价是第一帧发出后就失去"静默重放"的权利，之后失败只能续写。
            committed = true;
            buffer = [];
            committedText += textOf(ev);
            yield { kind: 'event', event: ev };
            continue;
          }

          buffer.push(ev);
          if (shouldCommit(ev, buffer, cfg.limits.commitChars)) {
            committed = true;
            for (const b of buffer) {
              committedText += textOf(b);
              yield { kind: 'event', event: b };
            }
            buffer = [];
            dataStartedAt ??= this.now();
          }
        }

        if (!sawTerminal) {
          throw new UpstreamFailure({
            providerId: target.id,
            reason: 'stream_aborted',
            message: 'stream ended without terminal event',
            phase: 'stream',
          });
        }

        this.deps.health.recordSuccess(target.id);
        log.info('stream served', {
          provider: target.id,
          attempts,
          ms: this.now() - startedAt,
          emittedChars: cpLen(committedText),
          configVersion: cfg.configVersion,
        });
        return;
      } catch (err) {
        const failure = asFailure(err, target.id);
        if (iterator) {
          try {
            await iterator.return?.(undefined);
          } catch {
            /* 关不掉就算了，AbortController 已经掐了连接 */
          }
        }

        if (clientSignal?.aborted) throw failure;
        this.deps.health.recordFailure(target.id, failure);
        last = failure;

        // 请求体本身非法：备用模型也会一样拒绝，换来换去只是白烧额度
        if (failure.abortsChain && cfg.policy.abortOnBadRequest) throw failure;

        if (!committed) {
          // ★ 情况 A：客户端一个字节都没收到 —— 静默重放给下一家。
          //   这就是"无缝"的来源：调用方连重试都不需要知道。
          buffer = [];
          log.warn('failover transparent', {
            from: target.id,
            reason: failure.reason,
            phase: failure.phase,
            attempts,
          });
          continue;
        }

        // ★ 情况 B：已经有内容落到客户端了。重放会造成重复文本，
        //   所以只能带着半截内容去下一家"续写"。
        log.error('stream broke after commit', {
          from: target.id,
          reason: failure.reason,
          phase: failure.phase,
          emittedChars: cpLen(committedText),
        });

        const nextTarget = chain[chain.indexOf(target) + 1];
        if (cfg.policy.salvageOnStreamBreak && nextTarget && !failure.abortsChain) {
          yield {
            kind: 'salvage',
            fromProviderId: target.id,
            toProviderId: nextTarget.id,
            reason: failure.reason,
          };
          currentReq = withContinuation(currentReq, committedText, cfg.policy.salvageInstruction);
          continue;
        }
        throw failure;
      } finally {
        clearTimeout(attemptTimer);
        detach();
      }
    }

    throw last ?? new UpstreamFailure({ providerId: 'none', reason: 'unknown', message: 'no candidate target available' });
  }
}
