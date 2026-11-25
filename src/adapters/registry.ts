/** 适配器注册表：新增厂商只需 register 一个实例，配置里写 adapter 名字即可。 */
import { UpstreamFailure } from '../core/errors.js';
import type { Adapter } from './adapter.js';
import { OpenAiCompatibleAdapter } from './openai.js';
import { AnthropicAdapter } from './anthropic.js';
import { CodexResponsesAdapter } from './codex.js';

export class AdapterRegistry {
  private readonly map = new Map<string, Adapter>();

  constructor(adapters: Adapter[] = [new OpenAiCompatibleAdapter(), new AnthropicAdapter(), new CodexResponsesAdapter()]) {
    for (const a of adapters) this.register(a);
  }

  register(adapter: Adapter): void {
    this.map.set(adapter.kind, adapter);
  }

  get(kind: string): Adapter | undefined {
    return this.map.get(kind);
  }

  kinds(): string[] {
    return [...this.map.keys()];
  }

  require(kind: string): Adapter {
    const adapter = this.map.get(kind);
    if (!adapter) {
      // 配置写错了 adapter 名字：属于"这一条 target 不可用"，跳过即可，不必终止链路
      throw new UpstreamFailure({
        providerId: `adapter:${kind}`,
        reason: 'unknown',
        message: `unknown adapter "${kind}", registered: ${this.kinds().join(', ')}`,
      });
    }
    return adapter;
  }
}
