/**
 * 统一失败语义。
 *
 * 网关里所有"要不要换模型"的决策，只依赖这里的 reason，不依赖各家平台的错误码
 * 文本 —— 这是适配器解耦的关键：新增一家模型只需要把它的错误体翻译成 reason。
 */

export type FailoverReason =
  | 'rate_limited' // 429 / 并发超限 / TPM 超限
  | 'quota_exhausted' // 额度耗尽、欠费、billing 未开通
  | 'auth_failed' // 401/403、key 失效、被封
  | 'timeout' // 连接超时、首字超时、流空闲超时
  | 'network_error' // DNS / RST / 5xx 网关级故障
  | 'upstream_5xx' // 上游 500/502/503/529
  | 'context_overflow' // 超上下文长度（换一家可能就能装下）
  | 'bad_request' // 请求体本身非法：换谁都白搭，直接终止链路
  | 'stream_aborted' // ★ 关键：HTTP 200 之后流被异常截断 / 流内 error 事件
  | 'unknown';

/** 换下一家还有意义的错误。 */
const RETRYABLE = new Set<FailoverReason>([
  'rate_limited',
  'quota_exhausted',
  'auth_failed',
  'timeout',
  'network_error',
  'upstream_5xx',
  'context_overflow',
  'stream_aborted',
  'unknown',
]);

/** 客户端自己写错了，重试只会把四家模型全烧一遍。 */
const CHAIN_ABORTING = new Set<FailoverReason>(['bad_request']);

export interface UpstreamFailureInit {
  providerId: string;
  reason: FailoverReason;
  message: string;
  status?: number;
  /** request=还没拿到响应头；stream=已开始吐流；parse=响应体解析失败 */
  phase?: 'request' | 'stream' | 'parse';
  retryAfterMs?: number;
  providerCode?: string;
  cause?: unknown;
}

export class UpstreamFailure extends Error {
  readonly providerId: string;
  readonly reason: FailoverReason;
  readonly status: number | undefined;
  readonly phase: 'request' | 'stream' | 'parse';
  readonly retryAfterMs: number | undefined;
  readonly providerCode: string | undefined;

  constructor(init: UpstreamFailureInit) {
    super(init.message, init.cause !== undefined ? { cause: init.cause } : undefined);
    this.name = 'UpstreamFailure';
    this.providerId = init.providerId;
    this.reason = init.reason;
    this.status = init.status;
    this.phase = init.phase ?? 'request';
    this.retryAfterMs = init.retryAfterMs;
    this.providerCode = init.providerCode;
  }

  get retryable(): boolean {
    return RETRYABLE.has(this.reason);
  }

  get abortsChain(): boolean {
    return CHAIN_ABORTING.has(this.reason);
  }

  toJSON(): Record<string, unknown> {
    return {
      provider: this.providerId,
      reason: this.reason,
      phase: this.phase,
      status: this.status,
      providerCode: this.providerCode,
      message: this.message,
    };
  }
}

// asFailure 放在 classifier.ts：它需要 classifyThrown，写在 errors.ts 会造成
// errors ↔ classifier 的循环依赖。
