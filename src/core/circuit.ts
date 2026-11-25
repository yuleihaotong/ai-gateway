/**
 * 熔断 / 冷却表。
 *
 * 目的不是"保护上游"（那是礼貌说法），而是避免某个已经 401 的 key 被
 * 每个请求都先试一次，把首字延迟从 300ms 拉到 3s。
 */
import type { UpstreamFailure, FailoverReason } from './errors.js';

export interface ProviderState {
  providerId: string;
  consecutiveFailures: number;
  openUntil: number;
  lastReason?: FailoverReason;
  lastStatus?: number;
  lastErrorAt?: number;
  lastErrorMessage?: string;
  totalOk: number;
  totalFail: number;
}

const BASE_COOLDOWN_MS: Record<FailoverReason, number> = {
  rate_limited: 30_000,
  quota_exhausted: 10 * 60_000,
  auth_failed: 15 * 60_000,
  timeout: 15_000,
  network_error: 20_000,
  upstream_5xx: 10_000,
  context_overflow: 0, // 换模型立刻有意义，不要冷却
  stream_aborted: 5_000,
  bad_request: 0,
  unknown: 30_000,
};

const MAX_COOLDOWN_MS = 30 * 60_000;

export class ProviderHealth {
  private readonly states = new Map<string, ProviderState>();

  constructor(private readonly now: () => number = Date.now) {}

  state(providerId: string): ProviderState {
    let s = this.states.get(providerId);
    if (!s) {
      s = { providerId, consecutiveFailures: 0, openUntil: 0, totalOk: 0, totalFail: 0 };
      this.states.set(providerId, s);
    }
    return s;
  }

  isAvailable(providerId: string): boolean {
    return this.state(providerId).openUntil <= this.now();
  }

  recordSuccess(providerId: string): void {
    const s = this.state(providerId);
    s.consecutiveFailures = 0;
    s.openUntil = 0;
    s.totalOk += 1;
    s.lastReason = undefined;
  }

  recordFailure(providerId: string, failure: UpstreamFailure): void {
    const s = this.state(providerId);
    s.consecutiveFailures += 1;
    s.totalFail += 1;
    s.lastReason = failure.reason;
    s.lastStatus = failure.status;
    s.lastErrorAt = this.now();
    s.lastErrorMessage = failure.message.slice(0, 300);

    const base = failure.retryAfterMs ?? BASE_COOLDOWN_MS[failure.reason] ?? 30_000;
    // 连续失败指数退避：5s → 10s → 20s，最多叠到 8 倍
    const escalated = base * Math.min(2 ** Math.max(0, s.consecutiveFailures - 1), 8);
    s.openUntil = this.now() + Math.min(escalated, MAX_COOLDOWN_MS);
  }

  snapshot(): ProviderState[] {
    return [...this.states.values()].map((s) => ({ ...s }));
  }

  reset(providerId?: string): void {
    if (providerId) this.states.delete(providerId);
    else this.states.clear();
  }
}
