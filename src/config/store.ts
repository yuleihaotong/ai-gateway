/**
 * 配置热重载。
 *
 * 三条设计约束：
 *  1) 原子替换：新配置校验通过才 swap；失败则保留旧配置，并把错误暴露在 /healthz。
 *  2) 请求内快照：网关在请求入口取一次 config.get()，整条候选链都基于这份快照，
 *     避免"重载发生在第 2 次重试时"导致候选链和 key 半新半旧。
 *  3) 密钥来自环境变量：JSON 里只写 ${DEEPSEEK_API_KEY}，重载时重新展开，
 *     所以轮换 key 只要改环境变量 + SIGHUP（或直接改文件触发）。
 */
import { readFile } from 'node:fs/promises';
import { watchFile, unwatchFile } from 'node:fs';
import { ConfigError, parseConfig, type GatewayConfig } from './schema.js';

export interface ReloadResult {
  ok: boolean;
  configVersion: number;
  error?: string;
}

type Listener = (cfg: GatewayConfig, version: number, previous: GatewayConfig) => void;

function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const v of Object.values(value as Record<string, unknown>)) deepFreeze(v);
  }
  return value;
}

async function loadFile(path: string, env: NodeJS.ProcessEnv, version: number): Promise<GatewayConfig> {
  let text: string;
  try {
    text = await readFile(path, 'utf8');
  } catch (err) {
    throw new ConfigError([`cannot read ${path}: ${(err as Error).message}`]);
  }
  // Windows 上记事本/PowerShell 存盘会带 UTF-8 BOM，JSON.parse 会当成非法字符
  text = text.replace(/^\uFEFF/, '');
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (err) {
    throw new ConfigError([`${path} is not valid JSON: ${(err as Error).message}`]);
  }
  const cfg = parseConfig(raw, env);
  return deepFreeze({ ...cfg, configVersion: version });
}

export class ConfigStore {
  private snapshot: GatewayConfig;
  private version: number;
  private lastError: string | undefined;
  private readonly listeners = new Set<Listener>();
  private debounce?: NodeJS.Timeout;
  private reloading = false;
  private queued = false;
  private watching = false;

  private constructor(
    readonly path: string,
    initial: GatewayConfig,
    private readonly env: NodeJS.ProcessEnv,
  ) {
    this.snapshot = initial;
    this.version = initial.configVersion;
  }

  static async open(path: string, env: NodeJS.ProcessEnv = process.env): Promise<ConfigStore> {
    const cfg = await loadFile(path, env, 0);
    const store = new ConfigStore(path, cfg, env);
    store.watch();
    return store;
  }

  /** 请求入口拿到的就是这份不可变快照。 */
  get(): GatewayConfig {
    return this.snapshot;
  }

  get configVersion(): number {
    return this.version;
  }

  get lastLoadError(): string | undefined {
    return this.lastError;
  }

  onChange(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async reload(): Promise<ReloadResult> {
    if (this.reloading) {
      this.queued = true;
      return { ok: false, configVersion: this.version, error: 'reload already in progress' };
    }
    this.reloading = true;
    try {
      const next = await loadFile(this.path, this.env, this.version + 1);
      const previous = this.snapshot;
      this.snapshot = next;
      this.version = next.configVersion;
      this.lastError = undefined;
      for (const listener of this.listeners) {
        try {
          listener(next, this.version, previous);
        } catch {
          /* 监听器自己炸了不能影响配置生效 */
        }
      }
      return { ok: true, configVersion: this.version };
    } catch (err) {
      // ★ 保留旧配置：宁可继续用旧 key 服务，也不要因为一次手滑把线上打穿
      this.lastError = err instanceof ConfigError ? err.message : (err as Error).message;
      return { ok: false, configVersion: this.version, error: this.lastError };
    } finally {
      this.reloading = false;
      if (this.queued) {
        this.queued = false;
        this.scheduleReload(50);
      }
    }
  }

  close(): void {
    if (this.watching) {
      unwatchFile(this.path);
      this.watching = false;
    }
    if (this.debounce) clearTimeout(this.debounce);
    this.listeners.clear();
  }

  private watch(): void {
    if (this.watching) return;
    this.watching = true;
    // watchFile 是轮询实现，对 vim/IDE 的"写临时文件再 rename"原子保存最稳
    watchFile(this.path, { interval: 250 }, () => this.scheduleReload(120));
  }

  private scheduleReload(delayMs: number): void {
    if (this.debounce) clearTimeout(this.debounce);
    this.debounce = setTimeout(() => {
      void this.reload().then((r) => {
        if (!r.ok) process.stderr.write(`[config] reload rejected: ${r.error}\n`);
      });
    }, delayMs);
    this.debounce.unref?.();
  }
}
