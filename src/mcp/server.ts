/**
 * MCP Server（stdio 传输，newline-delimited JSON-RPC 2.0）。
 *
 * 不引官方 SDK 是为了让这个网关零运行时依赖：`node dist/src/index.js mcp`
 * 就能被 Claude Desktop / Cursor / 任何 MCP 客户端拉起来。
 *
 * 暴露两个工具：
 *   gateway_chat   —— 走完整降级链做一次补全
 *   gateway_status —— 看当前候选链、冷却状态、配置版本
 */
import type { ConfigStore } from '../config/store.js';
import type { ProviderHealth } from '../core/circuit.js';
import type { Gateway } from '../routing/gateway.js';
import type { CanonMessage, CanonRequest } from '../protocol/types.js';
import { UpstreamFailure } from '../core/errors.js';

interface JsonRpcRequest {
  jsonrpc?: string;
  id?: string | number | null;
  method?: string;
  params?: unknown;
}

interface McpTextContent {
  type: 'text';
  text: string;
}

const SERVER_INFO = { name: 'ai-routing-gateway', version: '0.1.0' };

/**
 * 本服务端真正支持的协议版本（新 → 旧）。
 *
 * ★ 协商规则：客户端要的版本在列表里 → 回它；不在列表里 → 回我们自己的最高版本，
 *   由**客户端**决定是否继续。绝不能把未知版本原样回显 —— 那等于宣称自己支持
 *   一个从没实现过的协议，后面的字段语义全靠猜。
 */
export const SUPPORTED_PROTOCOLS = ['2025-06-18', '2025-03-26', '2024-11-05'] as const;
export const LATEST_PROTOCOL: string = SUPPORTED_PROTOCOLS[0] ?? '2025-06-18';

export interface ProtocolChoice {
  version: string;
  /** true = 客户端要的版本我们不支持，回落到了自己的最新版 */
  negotiatedDown: boolean;
}

export function negotiateProtocol(requested: unknown): ProtocolChoice {
  if (typeof requested === 'string' && (SUPPORTED_PROTOCOLS as readonly string[]).includes(requested)) {
    return { version: requested, negotiatedDown: false };
  }
  return { version: LATEST_PROTOCOL, negotiatedDown: true };
}

const TOOLS = [
  {
    name: 'gateway_chat',
    description: '通过动态路由网关做一次对话补全：主模型失败（429/额度/鉴权/断流）时自动落到备用模型池。',
    inputSchema: {
      type: 'object',
      properties: {
        messages: {
          type: 'array',
          description: 'OpenAI 风格消息数组',
          items: {
            type: 'object',
            properties: { role: { type: 'string' }, content: { type: 'string' } },
            required: ['role', 'content'],
          },
        },
        model: { type: 'string', description: '客户端模型别名，默认取配置里的 defaultModel' },
        temperature: { type: 'number' },
        max_tokens: { type: 'number' },
      },
      required: ['messages'],
    },
  },
  {
    name: 'gateway_status',
    description: '返回网关的候选链、各上游冷却状态和当前配置版本。',
    inputSchema: { type: 'object', properties: {} },
  },
];

function rpcResult(id: JsonRpcRequest['id'], result: unknown): string {
  return JSON.stringify({ jsonrpc: '2.0', id: id ?? null, result }) + '\n';
}

function rpcError(id: JsonRpcRequest['id'], code: number, message: string): string {
  return JSON.stringify({ jsonrpc: '2.0', id: id ?? null, error: { code, message } }) + '\n';
}

export interface McpDeps {
  gateway: Gateway;
  config: ConfigStore;
  health: ProviderHealth;
  out?: { write(chunk: string): unknown };
  /** 注入 stdio 便于测试；默认 process.stdin */
  input?: AsyncIterable<string>;
}

export async function runMcpServer(deps: McpDeps): Promise<void> {
  const out = deps.out ?? process.stdout;
  const send = (line: string): void => void out.write(line);

  const callTool = async (name: string, args: Record<string, unknown>): Promise<{ content: McpTextContent[]; isError: boolean }> => {
    if (name === 'gateway_status') {
      const cfg = deps.config.get();
      const status = {
        configVersion: deps.config.configVersion,
        configError: deps.config.lastLoadError ?? null,
        defaultModel: cfg.defaultModel,
        models: Object.fromEntries(
          Object.entries(cfg.models).map(([alias, route]) => [alias, route.targets.map((t) => t.id)]),
        ),
        providers: deps.health.snapshot(),
      };
      return { content: [{ type: 'text', text: JSON.stringify(status, null, 2) }], isError: false };
    }

    if (name === 'gateway_chat') {
      const rawMessages = Array.isArray(args['messages']) ? args['messages'] : [];
      const messages: CanonMessage[] = rawMessages
        .map((m) => (m ?? {}) as { role?: string; content?: string })
        .filter((m) => typeof m.role === 'string')
        .map((m) => ({ role: m.role as CanonMessage['role'], content: typeof m.content === 'string' ? m.content : '' }));

      if (messages.length === 0) {
        return { content: [{ type: 'text', text: 'messages 不能为空' }], isError: true };
      }

      const cfg = deps.config.get();
      const req: CanonRequest = {
        model: typeof args['model'] === 'string' ? args['model'] : cfg.defaultModel,
        messages,
        stream: false,
        ...(typeof args['temperature'] === 'number' ? { temperature: args['temperature'] } : {}),
        ...(typeof args['max_tokens'] === 'number' ? { maxTokens: args['max_tokens'] } : {}),
      };

      try {
        const result = await deps.gateway.complete(req);
        const meta = `\n\n— served by ${result.providerId} (attempt ${result.attempts}, config v${result.configVersion})`;
        return { content: [{ type: 'text', text: result.response.content + meta }], isError: false };
      } catch (err) {
        const failure = err instanceof UpstreamFailure ? err : new UpstreamFailure({ providerId: 'gateway', reason: 'unknown', message: String(err) });
        return {
          content: [{ type: 'text', text: `全部候选模型均失败：${JSON.stringify(failure.toJSON())}` }],
          isError: true,
        };
      }
    }

    throw new Error(`unknown tool: ${name}`);
  };

  const handle = async (msg: JsonRpcRequest): Promise<void> => {
    const isNotification = msg.id === undefined;
    if (isNotification) return; // notifications/initialized 等：无需响应

    switch (msg.method) {
      case 'initialize': {
        const params = (msg.params ?? {}) as { protocolVersion?: string };
        const negotiated = negotiateProtocol(params.protocolVersion);
        if (negotiated.negotiatedDown) {
          process.stderr.write(
            `[mcp] client requested unsupported protocolVersion ${JSON.stringify(params.protocolVersion ?? null)}; offering ${negotiated.version}\n`,
          );
        }
        send(
          rpcResult(msg.id, {
            protocolVersion: negotiated.version,
            capabilities: { tools: { listChanged: false } },
            serverInfo: SERVER_INFO,
            ...(negotiated.negotiatedDown
              ? { instructions: `Requested MCP protocol version is not supported; this server speaks ${negotiated.version}.` }
              : {}),
          }),
        );
        return;
      }
      case 'ping': {
        send(rpcResult(msg.id, {}));
        return;
      }
      case 'tools/list': {
        send(rpcResult(msg.id, { tools: TOOLS }));
        return;
      }
      case 'tools/call': {
        const params = (msg.params ?? {}) as { name?: string; arguments?: Record<string, unknown> };
        if (!params.name) {
          send(rpcError(msg.id, -32602, 'tools/call requires params.name'));
          return;
        }
        try {
          const result = await callTool(params.name, params.arguments ?? {});
          send(rpcResult(msg.id, result));
        } catch (err) {
          send(rpcError(msg.id, -32000, (err as Error).message));
        }
        return;
      }
      default: {
        send(rpcError(msg.id, -32601, `method not found: ${msg.method ?? ''}`));
      }
    }
  };

  let queue: Promise<void> = Promise.resolve();
  let buf = '';

  const input = deps.input ?? (process.stdin as unknown as AsyncIterable<string>);
  if (deps.input === undefined) process.stdin.setEncoding('utf8');
  for await (const chunk of input) {
    buf += chunk as string;
    for (;;) {
      const idx = buf.indexOf('\n');
      if (idx < 0) break;
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;

      let parsed: JsonRpcRequest;
      try {
        parsed = JSON.parse(line) as JsonRpcRequest;
      } catch {
        send(rpcError(null, -32700, 'parse error'));
        continue;
      }
      // 串行执行：MCP 客户端虽然会并发发请求，但工具内部要走供应商限速，
      // 并发跑会把 429 概率打满，反而更慢。
      queue = queue.then(() => handle(parsed)).catch(() => undefined);
    }
  }

  await queue;
}
