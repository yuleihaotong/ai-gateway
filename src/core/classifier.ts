/**
 * 鉴权捕获层：把"上游返回了什么"翻译成统一的 FailoverReason。
 *
 * 这一层的输入只有四种：HTTP 状态码、响应头、响应体文本、抛出的异常。
 * 新增模型厂商时，只有这里的 pattern 表可能需要补一行，路由层完全不用动。
 */
import { UpstreamFailure, type FailoverReason } from './errors.js';

/** 额度类：注意各家措辞差异很大，中英文都要覆盖。 */
const QUOTA_PATTERNS = [
  /insufficient[_ ]quota/i,
  /exceeded your current quota/i,
  /quota (exceeded|exhausted)/i,
  /billing (hard )?limit/i,
  /payment required/i,
  /余额不足/,
  /额度(已)?(用尽|耗尽|不足)/,
  /欠费/,
  /账户.{0,6}(停用|冻结)/,
];

const AUTH_PATTERNS = [
  /invalid[_ ]api[_ ]key/i,
  /incorrect api key/i,
  /invalid (token|authentication)/i,
  /authentication (failed|error)/i,
  /unauthori[sz]ed/i,
  /api key (not|is not) valid/i,
  /鉴权失败/,
  /无效.{0,4}(token|密钥|key)/i,
];

const CONTEXT_PATTERNS = [
  /context[_ ]length[_ ]exceeded/i,
  /maximum context length/i,
  /too many tokens/i,
  /prompt is too long/i,
  /exceeds? (the )?model.{0,20}(limit|window)/i,
  /超过.{0,8}(上|长度|窗口)/,
];

const NUMERIC_429_HINTS = [/rate[ _]limit/i, /too many requests/i, /tpm|rpm/i, /请求过于频繁/];

function bodyHas(patterns: RegExp[], body: string): boolean {
  return patterns.some((re) => re.test(body));
}

/** 支持 `retry-after: 30`、毫秒字符串、HTTP-date 三种写法。 */
export function parseRetryAfter(headers: Headers): number | undefined {
  const raw = headers.get('retry-after') ?? headers.get('x-ratelimit-reset-requests');
  if (!raw) return undefined;
  const asNumber = Number(raw);
  if (Number.isFinite(asNumber) && asNumber >= 0) {
    // 约定：小于 1000 视为秒（各家都是秒），否则视为毫秒
    return asNumber < 1000 ? asNumber * 1000 : asNumber;
  }
  const asDate = Date.parse(raw);
  if (!Number.isNaN(asDate)) return Math.max(0, asDate - Date.now());
  return undefined;
}

function pickProviderCode(body: unknown): string | undefined {
  if (!body || typeof body !== 'object') return undefined;
  const obj = body as Record<string, unknown>;
  const err = obj['error'];
  if (err && typeof err === 'object') {
    const e = err as Record<string, unknown>;
    const code = e['code'] ?? e['type'] ?? e['status'];
    if (typeof code === 'string') return code;
  }
  const code = obj['code'];
  return typeof code === 'string' ? code : undefined;
}

/**
 * HTTP 层错误 → 统一失败。
 * 判定顺序很重要：429 先看 body 是不是"额度用尽"（429 也可能代表欠费）。
 */
export function classifyHttpFailure(
  providerId: string,
  status: number,
  headers: Headers,
  bodyText: string,
): UpstreamFailure {
  let parsed: unknown;
  try {
    parsed = bodyText ? JSON.parse(bodyText) : undefined;
  } catch {
    parsed = undefined;
  }
  const providerCode = pickProviderCode(parsed);
  const msg = bodyText.slice(0, 800) || `HTTP ${status}`;
  const base = { providerId, message: msg, status, providerCode } as const;

  if (status === 429) {
    if (bodyHas(QUOTA_PATTERNS, bodyText)) {
      return new UpstreamFailure({ ...base, reason: 'quota_exhausted', retryAfterMs: parseRetryAfter(headers) });
    }
    return new UpstreamFailure({ ...base, reason: 'rate_limited', retryAfterMs: parseRetryAfter(headers) });
  }
  if (status === 401 || status === 403) {
    // 403 也可能是额度/权限未开通，两种都算"这家暂时不能用"
    const reason: FailoverReason = bodyHas(QUOTA_PATTERNS, bodyText) ? 'quota_exhausted' : 'auth_failed';
    return new UpstreamFailure({ ...base, reason });
  }
  if (status === 402) {
    return new UpstreamFailure({ ...base, reason: 'quota_exhausted' });
  }
  if (status === 408 || status === 504) {
    return new UpstreamFailure({ ...base, reason: 'timeout' });
  }
  if (status === 413 || status === 422) {
    return new UpstreamFailure({
      ...base,
      reason: bodyHas(CONTEXT_PATTERNS, bodyText) ? 'context_overflow' : 'bad_request',
    });
  }
  if (status === 400) {
    if (bodyHas(CONTEXT_PATTERNS, bodyText)) return new UpstreamFailure({ ...base, reason: 'context_overflow' });
    if (bodyHas(QUOTA_PATTERNS, bodyText)) return new UpstreamFailure({ ...base, reason: 'quota_exhausted' });
    if (bodyHas(AUTH_PATTERNS, bodyText)) return new UpstreamFailure({ ...base, reason: 'auth_failed' });
    return new UpstreamFailure({ ...base, reason: 'bad_request' });
  }
  if (status === 404 || status === 405) {
    // baseUrl / 路径配错了：跳过这一家，但不要终止整条链
    return new UpstreamFailure({ ...base, reason: 'unknown' });
  }
  if (status === 529) {
    return new UpstreamFailure({ ...base, reason: 'upstream_5xx' });
  }
  if (status >= 500) {
    return new UpstreamFailure({ ...base, reason: 'upstream_5xx' });
  }
  if (status >= 400) {
    return new UpstreamFailure({ ...base, reason: 'bad_request' });
  }
  return new UpstreamFailure({ ...base, reason: 'unknown' });
}

/** 网络层 / 中断异常 → 统一失败。 */
export function classifyThrown(providerId: string, err: unknown): UpstreamFailure {
  const e = err as { name?: string; code?: string; message?: string; cause?: { code?: string } } | null;
  const name = e?.name ?? '';
  const code = e?.code ?? e?.cause?.code ?? '';
  const message = e?.message ?? String(err);

  if (name === 'AbortError' || name === 'TimeoutError' || code === 'ABORT_ERR' || code === 'UND_ERR_ABORTED') {
    return new UpstreamFailure({ providerId, reason: 'timeout', message: `aborted: ${message}`, phase: 'request', cause: err });
  }
  if (code === 'ETIMEDOUT' || code === 'UND_ERR_CONNECT_TIMEOUT' || code === 'UND_ERR_HEADERS_TIMEOUT') {
    return new UpstreamFailure({ providerId, reason: 'timeout', message, phase: 'request', cause: err });
  }
  if (
    code === 'ECONNRESET' ||
    code === 'ECONNREFUSED' ||
    code === 'ENOTFOUND' ||
    code === 'EAI_AGAIN' ||
    code === 'EPIPE' ||
    code === 'UND_ERR_SOCKET' ||
    code === 'UND_ERR_CLOSED'
  ) {
    return new UpstreamFailure({ providerId, reason: 'network_error', message, phase: 'request', cause: err });
  }
  if (name === 'SyntaxError') {
    return new UpstreamFailure({ providerId, reason: 'stream_aborted', message: `malformed payload: ${message}`, phase: 'parse', cause: err });
  }
  return new UpstreamFailure({ providerId, reason: 'unknown', message, phase: 'request', cause: err });
}

/**
 * ★ 流内错误：HTTP 已经 200 了，但 SSE 里推了一条 error / 或者字段形状不对。
 * 这是"主模型中途挂掉"最常见也最容易被漏掉的一种，必须单独识别。
 */
export function classifyStreamPayload(providerId: string, payload: unknown): UpstreamFailure {
  const obj = (payload ?? {}) as Record<string, unknown>;
  const errObj =
    obj['error'] ??
    (obj['type'] === 'error' ? obj : undefined) ??
    (typeof obj['message'] === 'string' && obj['code'] ? obj : undefined);
  const text = typeof errObj === 'string' ? errObj : JSON.stringify(errObj ?? payload ?? 'stream error');

  let reason: FailoverReason = 'stream_aborted';
  if (/rate[ _]limit|too many requests|429/i.test(text)) reason = 'rate_limited';
  else if (bodyHas(QUOTA_PATTERNS, text)) reason = 'quota_exhausted';
  else if (/overloaded|unavailable|503|529/i.test(text)) reason = 'upstream_5xx';
  else if (bodyHas(CONTEXT_PATTERNS, text)) reason = 'context_overflow';

  return new UpstreamFailure({ providerId, reason, message: text.slice(0, 800), phase: 'stream' });
}

/** 任何异常 → 统一的失败语义。网关所有降级判断都从这里进。 */
export function asFailure(err: unknown, providerId: string): UpstreamFailure {
  if (err instanceof UpstreamFailure) return err;
  return classifyThrown(providerId, err);
}

export { QUOTA_PATTERNS, AUTH_PATTERNS, CONTEXT_PATTERNS, NUMERIC_429_HINTS };
