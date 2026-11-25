/**
 * 配置模型 + 校验。
 *
 * 一条硬规则：**配置非法时绝不半生效**。要么整份替换成功，要么保留上一份可用配置。
 * 之所以这么严，是因为热重载最怕的是"key 被清空但服务还在跑"，那会把线上请求全打到备用。
 */
import type { TargetSpec } from '../core/target.js';

export interface LimitsConfig {
  requestTimeoutMs: number;
  firstTokenTimeoutMs: number;
  idleTimeoutMs: number;
  totalTimeoutMs: number;
  /** 攒够多少个 **code point** 才提交（instant 模式下不生效） */
  commitChars: number;
  /** 攒了多久就提交（instant 模式下不生效） */
  commitDelayMs: number;
  /**
   * instant  = 第一帧（含 role chunk）立刻下发并提交：首字延迟 = 上游首字延迟。
   *            代价是此后失败只能 salvage 续写，不能静默重放。
   * buffered = 攒 commitChars / commitDelayMs 再提交：能零感知重放整段，但首字延迟
   *            上界是 commitChars × 上游每字符间隔（与上游速率成正比）。
   */
  commitMode: 'instant' | 'buffered';
  /** 是否把 `: gateway provider=... attempt=N` 这类注释写进客户端 SSE（默认关：那是运维信息，不是协议内容） */
  emitRouteComments: boolean;
  maxBodyBytes: number;
}

export interface PolicyConfig {
  maxAttempts: number;
  abortOnBadRequest: boolean;
  /** 流已经吐给客户端之后断了，是否用备用模型"续写" */
  salvageOnStreamBreak: boolean;
  salvageInstruction: string;
}

export interface ModelRouteConfig {
  /** priority=按配置顺序；round_robin=同优先级内轮询 */
  strategy: 'priority' | 'round_robin';
  targets: TargetSpec[];
}

export interface GatewayConfig {
  listen: { host: string; port: number };
  defaultModel: string;
  /** 客户端模型名 → models 里的键，便于灰度时改指向 */
  aliases: Record<string, string>;
  limits: LimitsConfig;
  policy: PolicyConfig;
  models: Record<string, ModelRouteConfig>;
  /** 每次成功加载自增，便于日志和 x-gateway-config 头对齐 */
  configVersion: number;
}

export class ConfigError extends Error {
  constructor(readonly problems: string[]) {
    super(`invalid gateway config:\n  - ${problems.join('\n  - ')}`);
    this.name = 'ConfigError';
  }
}

export const DEFAULT_LIMITS: LimitsConfig = {
  requestTimeoutMs: 60_000,
  firstTokenTimeoutMs: 20_000,
  idleTimeoutMs: 30_000,
  totalTimeoutMs: 300_000,
  commitChars: 24,
  commitDelayMs: 400,
  // 默认 instant：宁可在断流时续写一次，也不要让每个请求都无脑多等 0.5–1s。
  commitMode: 'instant',
  emitRouteComments: false,
  maxBodyBytes: 8 * 1024 * 1024,
};

export const DEFAULT_POLICY: PolicyConfig = {
  maxAttempts: 4,
  abortOnBadRequest: true,
  salvageOnStreamBreak: true,
  salvageInstruction:
    '上一条回复在上游服务中断处被截断。请严格从中断处继续输出剩余内容，不要重复已输出的部分，不要添加任何解释或前缀。',
};

const ENV_PATTERN = /\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}/g;

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/** 递归展开 ${VAR} / ${VAR:-default}。缺失且无默认值 → 记为配置错误。 */
export function expandEnvDeep(value: unknown, env: NodeJS.ProcessEnv, problems: string[], path = '$'): unknown {
  if (typeof value === 'string') {
    return value.replace(ENV_PATTERN, (_all, name: string, fallback?: string) => {
      const found = env[name];
      if (found !== undefined && found !== '') return found;
      if (fallback !== undefined) return fallback;
      problems.push(`${path}: environment variable ${name} is not set`);
      return '';
    });
  }
  if (Array.isArray(value)) return value.map((v, i) => expandEnvDeep(v, env, problems, `${path}[${i}]`));
  if (isRecord(value)) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = expandEnvDeep(v, env, problems, `${path}.${k}`);
    return out;
  }
  return value;
}

function readNumber(raw: Record<string, unknown>, key: string, fallback: number, problems: string[], path: string): number {
  const v = raw[key];
  if (v === undefined) return fallback;
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
    problems.push(`${path}.${key} must be a non-negative number`);
    return fallback;
  }
  return v;
}

function readString(raw: Record<string, unknown>, key: string, fallback: string, problems: string[], path: string): string {
  const v = raw[key];
  if (v === undefined) return fallback;
  if (typeof v !== 'string' || v.length === 0) {
    problems.push(`${path}.${key} must be a non-empty string`);
    return fallback;
  }
  return v;
}

function readBool(raw: Record<string, unknown>, key: string, fallback: boolean, problems: string[], path: string): boolean {
  const v = raw[key];
  if (v === undefined) return fallback;
  if (typeof v !== 'boolean') {
    problems.push(`${path}.${key} must be a boolean`);
    return fallback;
  }
  return v;
}

function readEnum<T extends string>(
  raw: Record<string, unknown>,
  key: string,
  allowed: readonly T[],
  fallback: T,
  problems: string[],
  path: string,
): T {
  const v = raw[key];
  if (v === undefined) return fallback;
  if (typeof v !== 'string' || !allowed.includes(v as T)) {
    problems.push(`${path}.${key} must be one of ${allowed.join(' | ')}`);
    return fallback;
  }
  return v as T;
}

let legacyCommitBytesWarned = false;

function parseTarget(raw: unknown, path: string, problems: string[], seen: Set<string>): TargetSpec | null {
  if (!isRecord(raw)) {
    problems.push(`${path} must be an object`);
    return null;
  }
  const id = typeof raw['id'] === 'string' ? raw['id'] : '';
  if (!id) problems.push(`${path}.id is required`);
  else if (seen.has(id)) problems.push(`${path}.id "${id}" is duplicated (provider ids must be globally unique)`);
  if (id) seen.add(id);

  const adapter = typeof raw['adapter'] === 'string' ? raw['adapter'] : '';
  if (!adapter) problems.push(`${path}.adapter is required (openai | anthropic | codex | <registered>)`);

  const baseUrl = typeof raw['baseUrl'] === 'string' ? raw['baseUrl'] : '';
  if (!baseUrl) problems.push(`${path}.baseUrl is required`);

  const model = typeof raw['model'] === 'string' ? raw['model'] : '';
  if (!model) problems.push(`${path}.model is required`);

  const apiKey = typeof raw['apiKey'] === 'string' ? raw['apiKey'] : '';
  if (!apiKey) problems.push(`${path}.apiKey is required (tip: use "\${YOUR_KEY_ENV}")`);

  const headers = isRecord(raw['headers'])
    ? Object.fromEntries(Object.entries(raw['headers']).map(([k, v]) => [k, String(v)]))
    : undefined;

  const tags = Array.isArray(raw['tags']) ? raw['tags'].filter((t): t is string => typeof t === 'string') : undefined;

  const target: TargetSpec = { id, adapter, baseUrl, apiKey, model };
  if (typeof raw['timeoutMs'] === 'number' && raw['timeoutMs'] > 0) target.timeoutMs = raw['timeoutMs'];
  if (typeof raw['weight'] === 'number') target.weight = raw['weight'];
  if (raw['enabled'] === false) target.enabled = false;
  if (headers) target.headers = headers;
  if (tags) target.tags = tags;
  if (isRecord(raw['extraBody'])) target.extraBody = raw['extraBody'];
  return id && adapter && baseUrl && model ? target : null;
}

export function parseConfig(rawInput: unknown, env: NodeJS.ProcessEnv = process.env): GatewayConfig {
  const problems: string[] = [];
  const raw = expandEnvDeep(rawInput, env, problems);
  if (!isRecord(raw)) throw new ConfigError(['config root must be a JSON object']);

  const listenRaw = isRecord(raw['listen']) ? raw['listen'] : {};
  const listen = {
    host: readString(listenRaw, 'host', '127.0.0.1', problems, '$.listen'),
    port: readNumber(listenRaw, 'port', 8787, problems, '$.listen'),
  };

  const limitsRaw = isRecord(raw['limits']) ? raw['limits'] : {};
  // 旧键兼容：commitBytes 在 v1 里是"字节数"，实际按 UTF-16 code unit 计数。
  // 现在统一叫 commitChars 且按 code point 计数，旧配置照常能跑（warn 一次）。
  if (limitsRaw['commitChars'] === undefined && limitsRaw['commitBytes'] !== undefined && !legacyCommitBytesWarned) {
    legacyCommitBytesWarned = true;
    process.stderr.write('[config] limits.commitBytes is deprecated and counts code units; use limits.commitChars (code points)\n');
  }
  const commitCharsFallback =
    limitsRaw['commitChars'] === undefined && typeof limitsRaw['commitBytes'] === 'number'
      ? (limitsRaw['commitBytes'] as number)
      : DEFAULT_LIMITS.commitChars;
  const limits: LimitsConfig = {
    requestTimeoutMs: readNumber(limitsRaw, 'requestTimeoutMs', DEFAULT_LIMITS.requestTimeoutMs, problems, '$.limits'),
    firstTokenTimeoutMs: readNumber(limitsRaw, 'firstTokenTimeoutMs', DEFAULT_LIMITS.firstTokenTimeoutMs, problems, '$.limits'),
    idleTimeoutMs: readNumber(limitsRaw, 'idleTimeoutMs', DEFAULT_LIMITS.idleTimeoutMs, problems, '$.limits'),
    totalTimeoutMs: readNumber(limitsRaw, 'totalTimeoutMs', DEFAULT_LIMITS.totalTimeoutMs, problems, '$.limits'),
    commitChars: readNumber(limitsRaw, 'commitChars', commitCharsFallback, problems, '$.limits'),
    commitDelayMs: readNumber(limitsRaw, 'commitDelayMs', DEFAULT_LIMITS.commitDelayMs, problems, '$.limits'),
    commitMode: readEnum(limitsRaw, 'commitMode', ['instant', 'buffered'] as const, DEFAULT_LIMITS.commitMode, problems, '$.limits'),
    emitRouteComments: readBool(limitsRaw, 'emitRouteComments', DEFAULT_LIMITS.emitRouteComments, problems, '$.limits'),
    maxBodyBytes: readNumber(limitsRaw, 'maxBodyBytes', DEFAULT_LIMITS.maxBodyBytes, problems, '$.limits'),
  };

  const policyRaw = isRecord(raw['policy']) ? raw['policy'] : {};
  const policy: PolicyConfig = {
    maxAttempts: readNumber(policyRaw, 'maxAttempts', DEFAULT_POLICY.maxAttempts, problems, '$.policy'),
    abortOnBadRequest: policyRaw['abortOnBadRequest'] !== false,
    salvageOnStreamBreak: policyRaw['salvageOnStreamBreak'] !== false,
    salvageInstruction: readString(policyRaw, 'salvageInstruction', DEFAULT_POLICY.salvageInstruction, problems, '$.policy'),
  };

  const modelsRaw = isRecord(raw['models']) ? raw['models'] : {};
  const models: Record<string, ModelRouteConfig> = {};
  const seenIds = new Set<string>();

  for (const [alias, modelRaw] of Object.entries(modelsRaw)) {
    const path = `$.models.${alias}`;
    if (!isRecord(modelRaw)) {
      problems.push(`${path} must be an object`);
      continue;
    }
    const targetsRaw = Array.isArray(modelRaw['targets']) ? modelRaw['targets'] : [];
    if (targetsRaw.length === 0) problems.push(`${path}.targets must contain at least one target`);

    const targets: TargetSpec[] = [];
    for (const [i, t] of targetsRaw.entries()) {
      const parsed = parseTarget(t, `${path}.targets[${i}]`, problems, seenIds);
      if (parsed) targets.push(parsed);
    }

    const strategy = modelRaw['strategy'] === 'round_robin' ? 'round_robin' : 'priority';
    models[alias] = { strategy, targets };
  }

  if (Object.keys(models).length === 0) problems.push('$.models must define at least one model route');

  const defaultModel =
    typeof raw['defaultModel'] === 'string' && raw['defaultModel'].length > 0
      ? raw['defaultModel']
      : Object.keys(models)[0] ?? '';

  if (defaultModel && !models[defaultModel]) {
    problems.push(`$.defaultModel "${defaultModel}" does not exist in $.models`);
  }

  const aliasesRaw = isRecord(raw['aliases']) ? raw['aliases'] : {};
  const aliases: Record<string, string> = {};
  for (const [from, to] of Object.entries(aliasesRaw)) {
    if (typeof to !== 'string' || !to) {
      problems.push(`$.aliases.${from} must be a string`);
      continue;
    }
    aliases[from] = to;
  }

  if (problems.length > 0) throw new ConfigError(problems);

  return {
    listen,
    defaultModel,
    aliases,
    limits,
    policy,
    models,
    configVersion: 0,
  };
}
