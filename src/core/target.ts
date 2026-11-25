/** 路由目标：一个"可被调用一次的上游端点"。配置里的一条 target 就是它。 */
export interface TargetSpec {
  id: string;
  /** 适配器类型：openai / anthropic / codex，可注册新的 */
  adapter: string;
  baseUrl: string;
  apiKey: string;
  model: string;
  timeoutMs?: number;
  headers?: Record<string, string>;
  enabled?: boolean;
  tags?: string[];
  weight?: number;
  /** 厂商专属的固定请求字段，例如 GLM 的 do_sample */
  extraBody?: Record<string, unknown>;
}
