/**
 * 候选链解析 = 路由策略层。
 *
 * 这里只回答一个问题："这次请求按什么顺序打哪些 target"。
 * 具体怎么请求、怎么解析，是 Adapter 的事；什么时候放弃，是 Gateway 的事。
 */
import type { GatewayConfig } from '../config/schema.js';
import type { TargetSpec } from '../core/target.js';
import type { ProviderHealth } from '../core/circuit.js';
import type { CanonRequest } from '../protocol/types.js';
import { UpstreamFailure } from '../core/errors.js';

/** round_robin 的游标：按"客户端模型别名"分开计数，避免互相干扰 */
const rrCursor = new Map<string, number>();

export function resolveModelAlias(cfg: GatewayConfig, requested: string): string {
  return cfg.aliases[requested] ?? requested;
}

export function resolveChain(cfg: GatewayConfig, req: CanonRequest, health: ProviderHealth): TargetSpec[] {
  const alias = resolveModelAlias(cfg, req.model);
  const route = cfg.models[alias] ?? cfg.models[cfg.defaultModel];

  if (!route) {
    throw new UpstreamFailure({
      providerId: 'config',
      reason: 'bad_request',
      message: `model "${req.model}" is not configured and no defaultModel is available`,
    });
  }

  let targets = route.targets.filter((t) => t.enabled !== false);
  if (targets.length === 0) {
    throw new UpstreamFailure({
      providerId: 'config',
      reason: 'bad_request',
      message: `model "${alias}" has no enabled targets`,
    });
  }

  if (route.strategy === 'round_robin' && targets.length > 1) {
    const cursor = rrCursor.get(alias) ?? 0;
    rrCursor.set(alias, cursor + 1);
    const offset = cursor % targets.length;
    targets = [...targets.slice(offset), ...targets.slice(0, offset)];
  }

  const healthy = targets.filter((t) => health.isAvailable(t.id));
  // ★ 全都处于冷却期的极端情况：仍然按顺序试探一遍。
  //   直接返回 503 对调用方毫无价值，试一次成功概率总大于 0。
  return healthy.length > 0 ? healthy : targets;
}

/**
 * ★ 断流续写：把"已经发给客户端的半截文本"作为 assistant 前缀塞回去，
 * 再补一条 user 指令让备用模型接着写。
 *
 * 为什么用 user 而不是 system：多数厂商会拒绝 "assistant, assistant" 连续两轮，
 * 而 user-role 的续写指令在所有厂商都合法；这条消息只存在于上游上下文，
 * 不会回灌给客户端，所以调用方看不到。
 */
export function withContinuation(req: CanonRequest, partialText: string, instruction: string): CanonRequest {
  if (!partialText) return req;
  return {
    ...req,
    // 续写不需要工具调用，避免备用模型从头再调一遍工具
    tools: req.tools,
    messages: [
      ...req.messages,
      { role: 'assistant', content: partialText },
      { role: 'user', content: instruction },
    ],
  };
}
