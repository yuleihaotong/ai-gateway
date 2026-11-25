import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  classifyHttpFailure,
  classifyThrown,
  classifyStreamPayload,
  parseRetryAfter,
} from '../src/core/classifier.js';
import { UpstreamFailure } from '../src/core/errors.js';
import { ProviderHealth } from '../src/core/circuit.js';

const H = (init: Record<string, string> = {}): Headers => new Headers(init);

test('429 带"余额不足"必须判成额度耗尽而不是限流', () => {
  const f = classifyHttpFailure('a', 429, H(), JSON.stringify({ error: { message: 'Insufficient balance: 余额不足' } }));
  assert.equal(f.reason, 'quota_exhausted');
  assert.equal(f.abortsChain, false);
  assert.equal(f.retryable, true);
});

test('429 纯限流 → rate_limited，并解析 retry-after 秒', () => {
  const f = classifyHttpFailure('a', 429, H({ 'retry-after': '7' }), JSON.stringify({ error: { message: 'Rate limit reached' } }));
  assert.equal(f.reason, 'rate_limited');
  assert.equal(f.retryAfterMs, 7000);
});

test('401/403 → auth_failed；403 带额度文案 → quota_exhausted', () => {
  assert.equal(classifyHttpFailure('a', 401, H(), '{"error":{"message":"invalid api key"}}').reason, 'auth_failed');
  assert.equal(classifyHttpFailure('a', 403, H(), '{"error":{"message":"account 欠费"}}').reason, 'quota_exhausted');
});

test('400 的上下文超限可以换一家，其他 400 直接终止链路', () => {
  const overflow = classifyHttpFailure('a', 400, H(), '{"error":{"message":"This model\'s maximum context length is 8192 tokens"}}');
  assert.equal(overflow.reason, 'context_overflow');
  const fatal = classifyHttpFailure('a', 400, H(), '{"error":{"message":"invalid parameter: temperature"}}');
  assert.equal(fatal.reason, 'bad_request');
  assert.equal(fatal.abortsChain, true);
});

test('5xx / 404 的语义：5xx 可换家，404 说明这一家配错了但也不终止链路', () => {
  assert.equal(classifyHttpFailure('a', 529, H(), 'overloaded').reason, 'upstream_5xx');
  assert.equal(classifyHttpFailure('a', 500, H(), '').reason, 'upstream_5xx');
  assert.equal(classifyHttpFailure('a', 404, H(), '').reason, 'unknown');
});

test('parseRetryAfter 支持秒、毫秒与 HTTP-date', () => {
  assert.equal(parseRetryAfter(H({ 'retry-after': '3' })), 3000);
  assert.equal(parseRetryAfter(H({ 'retry-after': '2500' })), 2500);
  const date = new Date(Date.now() + 5000).toUTCString();
  const ms = parseRetryAfter(H({ 'retry-after': date }));
  assert.ok(ms !== undefined && ms > 2000 && ms <= 6000, `got ${String(ms)}`);
  assert.equal(parseRetryAfter(H()), undefined);
});

test('异常分类：超时/网络/解析分别映射到不同 reason', () => {
  const timeout = classifyThrown('a', Object.assign(new Error('aborted'), { name: 'TimeoutError' }));
  assert.equal(timeout.reason, 'timeout');
  const net = classifyThrown('a', Object.assign(new Error('fetch failed'), { cause: { code: 'ECONNRESET' } }));
  assert.equal(net.reason, 'network_error');
  const dns = classifyThrown('a', { code: 'ENOTFOUND', message: 'getaddrinfo' });
  assert.equal(dns.reason, 'network_error');
  const syntax = classifyThrown('a', new SyntaxError('Unexpected token'));
  assert.equal(syntax.reason, 'stream_aborted');
});

test('流内 error 事件能识别成额度耗尽', () => {
  const f = classifyStreamPayload('a', { error: { message: 'you exceeded your current quota' } });
  assert.equal(f.reason, 'quota_exhausted');
  assert.equal(f.phase, 'stream');
});

test('冷却：额度耗尽冷却 10 分钟，成功后立刻恢复', () => {
  let now = 1_000_000;
  const health = new ProviderHealth(() => now);
  const failure = new UpstreamFailure({ providerId: 'a', reason: 'quota_exhausted', message: 'no balance' });

  health.recordFailure('a', failure);
  assert.equal(health.isAvailable('a'), false);

  now += 60_000;
  assert.equal(health.isAvailable('a'), false, '1 分钟后仍然冷却');

  now += 10 * 60_000;
  assert.equal(health.isAvailable('a'), true, '10 分钟后恢复');

  health.recordFailure('a', failure);
  health.recordSuccess('a');
  assert.equal(health.state('a').consecutiveFailures, 0);
  assert.equal(health.isAvailable('a'), true);
});
