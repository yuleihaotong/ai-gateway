# AI 模型动态路由网关

一个**零运行时依赖**的 TypeScript 网关：对外暴露 OpenAI 兼容接口（或作为 MCP Server），对内维护一条
「主模型 → 备用池」的降级链。主模型 429 / 额度耗尽 / key 失效 / 超时 / **流被掐断** 时，网关自己换家，
调用方完全无感知。

```
client ──► 接入层 (HTTP / MCP)
             │  ①解析请求 → 统一 IR
             ▼
           路由层 (policy + gateway)
             │  ②算候选链  ③软提交决策  ④续写编排
             ▼
           协议转换层 (adapter: openai / anthropic / codex)
             │  ⑤IR → 厂商协议；厂商响应 → IR 事件流
             ▼
           上游模型
             │  异常
             ▼
           鉴权捕获层 (classifier + circuit)
                ⑥状态码/错误体/流内 error → FailoverReason → 冷却表
```

一条请求的完整路径：

```
POST /v1/chat/completions
  └─ parseOpenAiRequest        → CanonRequest（客户端模型名保持原样）
  └─ resolveChain(cfg, req)    → [codex-main, deepseek-backup, glm-backup, claude-backup]
  └─ for target of chain:
        adapter.build()        → 各家 HTTP 请求
        adapter.decodeStream() → 统一 StreamEvent
        gateway 软提交 / 失败分类 / 换家 / 续写
  └─ OpenAiChunkEncoder        → 客户端看到的永远是同一套 OpenAI chunk（id/model 都是别名）
```

## 目录结构

```
src/
  index.ts                  # 入口：serve / mcp / check 三种形态
  routing/
    policy.ts               # 路由策略层：别名解析、候选链（priority / round_robin）、断点续写上下文
    gateway.ts              # ★ 核心：非流式整包重试、流式软提交 + 透明切换 + salvage 续写
  core/
    classifier.ts           # 鉴权捕获层：429/401/402/400/5xx/网络异常/流内 error → FailoverReason
    circuit.ts              # 冷却/熔断表：按 reason 分级 + 指数退避 + retry-after 优先
    errors.ts               # FailoverReason 与 UpstreamFailure（唯一失败语义）
    target.ts               # TargetSpec：一家上游的配置结构
  protocol/
    types.ts                # IR：CanonRequest / CanonResponse / StreamEvent
    sse.ts                  # SSE 分帧器（增量、跨 chunk、CRLF）与 SseWriter
    openai-in.ts            # 客户端请求 → IR
    openai-out.ts           # IR → OpenAI chunk/响应/错误体
  adapters/
    adapter.ts              # 适配器契约 + BaseAdapter
    openai.ts               # OpenAI 兼容（DeepSeek / GLM / Kimi / vLLM / one-api…）
    anthropic.ts            # Claude Messages API
    codex.ts                # OpenAI Responses API（Codex 系）
    registry.ts             # kind → adapter
  config/
    schema.ts               # 校验 + ${ENV} 展开 + 默认值，一次性收集所有错误
    store.ts                # 热重载：原子替换、失败保留旧配置、请求内快照
  mcp/server.ts             # MCP Server（stdio JSON-RPC）：gateway_chat / gateway_status
scripts/fake-upstream.mjs   # 本地假上游，演示降级用（不需要真实 key）
test/                       # 38 个用例：分类、SSE、适配器、切换、续写、热重载
```

## 三大业务逻辑怎么落地

### 1. 主备无缝切换：失败必须是"可分类的"

切换的前提不是"catch 住异常",而是知道**这次失败值不值得换家**。所有异常先归一到 10 种 reason：

| reason | 典型来源 | 换家 | 冷却 |
| --- | --- | --- | --- |
| `rate_limited` | 429 + Rate limit | ✅ | 30s（或 retry-after） |
| `quota_exhausted` | 429 + insufficient balance / 402 / 欠费 | ✅ | 10min |
| `auth_failed` | 401 / 403 + invalid key | ✅ | 15min |
| `timeout` | 408 / 504 / 首字超时 / 空闲超时 | ✅ | 15s |
| `network_error` | ECONNRESET / ENOTFOUND / EPIPE | ✅ | 20s |
| `upstream_5xx` | 500 / 502 / 503 / 529 | ✅ | 10s |
| `context_overflow` | context_length_exceeded / 413 | ✅ | 不冷却 |
| `stream_aborted` | **200 之后流里没有结束标记就 EOF** | ✅ | 5s |
| `bad_request` | 请求体本身非法（参数错） | ❌ 终止整链 | 不冷却 |
| `unknown` | 404/405（baseUrl 配错）、无法归类 | ✅ | 30s |

`bad_request` 是唯一会**终止整条链**的：备用模型对同样的非法请求也只会回 400，换来换去纯烧额度。

冷却表的意义不是"保护上游"，而是别让一个已经 401 的 key 在每个请求上都先试一次，把首字延迟从
300ms 拖到 3s。冷却中的 target 会被 `resolveChain` 过滤掉；**但如果全部都在冷却，仍然按顺序全量返回**
——试一次的成功率总大于直接 503。

### 2. 流式转换：难点在"响应头一旦发出，状态码就焊死了"

非流式很干净：整包失败 → 整包换下一家 → 返回。流式不一样：

```
客户端已经收到 "HTTP/1.1 200" + 前 40 个字
        ↓  上游在这时断了
此时再返回 429 已经不合法（响应头发过了）
重放给下一家 → 客户端看到开头重复两遍
```

所以引入 **软提交（soft commit）**，它有两档（`limits.commitMode`）：

**`instant`（默认）**：第一个非文本事件（`start` / `role`）到达就立刻 `writeHead(200)`、立刻写给客户端，
并**立刻提交**。之后上游再失败就只剩"续写"一条路（不能静默重放），换来的是与直连几乎相同的首字延迟。

**`buffered`**：事件先攒在 `buffer` 里，攒够 `commitChars`（默认 24 个 **code point**，不是 UTF-16 code unit）
或等满 `commitDelayMs`（默认 400ms）才提交。适合"必须零感知重放"的场景。

- **提交前失败** → 丢弃 buffer，换下一家重放，客户端一个字节都没收到 ⇒ 完全透明；
- **提交后失败** → 不能重放，改为 **salvage 续写**：把已经吐出去的半截文本作为 `assistant` 消息塞回上下文，
  再补一条 `user` 续写指令，让备用模型"接着说"。

> `buffered` 的首字延迟上界是 **`commitChars × 上游每字符间隔`**，与上游速率成正比：
> 20ms/token 时 ≈0.5s，40ms/token 时 ≈1s。它不是"封顶 400ms"——`commitDelayMs` 只是兜底，
> 攒够字数会先到。要保住逐字感，用默认的 `instant`。
>
> 另一条硬规则：`commitDelayMs` 到点**只 flush，绝不 abort 上游**。允许 abort 并记 `timeout` 的只有两处：
> 一个字节都没来时的 `firstTokenTimeoutMs`，以及已提交后 `idleTimeoutMs` 内无任何数据。
> （早期版本在 flush 分支里顺手 abort 掉了连接，把"健康但慢"的上游判成断流，白烧一次重发。）

```ts
// src/routing/gateway.ts（节选，注释是设计的一部分）
if (!committed) {
  // ★ 情况 A：客户端一个字节都没收到 —— 静默重放给下一家。
  buffer = [];
  continue;
}

// ★ 情况 B：已经有内容落到客户端了。重放会造成重复文本，
//   所以只能带着半截内容去下一家"续写"。
const nextTarget = chain[chain.indexOf(target) + 1];
if (cfg.policy.salvageOnStreamBreak && nextTarget && !failure.abortsChain) {
  yield { kind: 'salvage', fromProviderId: target.id, toProviderId: nextTarget.id, reason: failure.reason };
  currentReq = withContinuation(currentReq, committedText, cfg.policy.salvageInstruction);
  continue;
}
throw failure;
```

续写为什么用 `user` 而不是 `system`：多数厂商会拒绝连续的 `assistant` 回合，而 user-role 的续写指令在所有
厂商都合法；这条消息只存在于上游上下文，不会回灌客户端，所以调用方看不见。

流量侧的两个细节：

- **第一帧真实内容到位后才 `writeHead(200)`**。这样"所有候选都失败"时，网关还能返回真实的 429/502，
  而不是先 200 再在流里塞个错误（那会让客户端 SDK 拿到一个"成功的空回答"）。
- **谁服务的写在响应头里，不写在流里**：流式和流式之外都带 `x-gateway-served-by` / `x-gateway-attempts`。
  `: gateway provider=…` 这类 SSE 注释行默认**关闭**（`limits.emitRouteComments`）——
  网关的承诺是"调用方以为一直在和同一个模型对话"，把供应商名写进正文字节流等于自己拆自己的台。
  排障时打开它，`curl -N` 就能看见换家轨迹。
- **写流要认背压**：`res.write()` 返回 `false` 时 `SseWriter` 会挂起等待 `drain`，帧顺序靠一条 promise 链
  保证；客户端断开则立刻 `abort` 上游，不让剩余帧继续往内存里解。

### 3. 协议适配：适配器只做四件事

```ts
export interface Adapter {
  readonly kind: string;
  build(target, req, stream): BuiltRequest;            // IR → 该厂商的 HTTP 请求
  decodeJson(target, json): CanonResponse;             // 非流式响应 → IR
  decodeStream(target, frames): AsyncGenerator<StreamEvent>; // SSE → 统一事件流（★ 必须能识别截断）
  decodeError(target, status, headers, bodyText): UpstreamFailure; // 默认实现已覆盖 90%
}
```

统一事件是 `start | text | reasoning | tool_call | usage | stop | error`。三个内置适配器的差异被完全吃掉：

| | OpenAI 兼容 | Anthropic | Codex (Responses) |
| --- | --- | --- | --- |
| 路径 | `POST /chat/completions` | `POST /messages` | `POST /responses` |
| 鉴权 | `authorization: Bearer` | `x-api-key` + `anthropic-version` | `authorization: Bearer` |
| system | messages 里一条 | 提升为顶层 `system` | 提升为顶层 `instructions` |
| 文本增量 | `choices[0].delta.content` | `content_block_delta.text_delta` | `response.output_text.delta` |
| 结束标记 | `finish_reason` / `[DONE]` | `message_stop` | `response.completed` |
| 截断判定 | 没有前两者就 EOF | 无 `message_stop` | 无 `completed/failed` |

**「截断判定」是整条降级链的触发器**，三个适配器都必须实现它。以 OpenAI 为例：

```ts
// src/adapters/openai.ts（结尾）
// ★ 上游 200 了、但没有任何"结束标记"就 EOF —— 这是最危险的静默截断。
//   必须抛出来，让路由层决定是"重放给下一家"还是"续写"。
if (!sawTerminal) {
  throw new UpstreamFailure({
    providerId: target.id,
    reason: 'stream_aborted',
    message: 'SSE closed without finish_reason or [DONE]',
    phase: 'stream',
  });
}
```

接一家新模型 = 实现这四个方法 + 在 `AdapterRegistry` 里 `register()` + 配置里写 `"adapter": "新的kind"`。
路由层、冷却层、协议转换层一行都不用改。

### 4. 配置热重载：绝不半生效

三条约束（`src/config/store.ts`）：

1. **原子替换**：新配置**全部**校验通过才 swap；任何一条错（含缺失的 `${ENV}`）都整体拒绝，
   旧配置继续服务，错误挂在 `lastLoadError` 和 `/healthz` 上。
2. **请求内快照**：网关在请求入口 `config.get()` 取一次不可变快照（`deepFreeze`），整条候选链都基于这份
   快照 —— 避免"重载正好发生在第 2 次重试时"，导致候选链和 key 半新半旧。
3. **密钥来自环境变量**：JSON 里只写 `${DEEPSEEK_API_KEY}`，每次重载重新展开，轮换 key 不用改文件。

触发方式三选一：改文件（`watchFile` 轮询，兼容 vim/VSCode 的"写临时文件再 rename"）、`SIGHUP`、
`POST /admin/reload`。Windows 记事本存的 BOM 会被自动剥掉。

## 快速开始

```bash
# 1. 依赖（仅编译期）：typescript + @types/node
pnpm install

# 2. 配置
cp config/gateway.example.json config/gateway.json
export CODEX_API_KEY=... DEEPSEEK_API_KEY=... GLM_API_KEY=... ANTHROPIC_API_KEY=...

# 3. 构建 & 自检
pnpm build
node dist/src/index.js check

# 4. 起服务
node dist/src/index.js serve          # HTTP 中间件，默认 127.0.0.1:8787
node dist/src/index.js mcp            # MCP Server（stdio）
```

调用方代码一行都不用改：

```bash
curl -N http://127.0.0.1:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

`gpt-4o` 通过 `aliases` 指到 `gpt-5-codex` 这条链；主模型 429 后，客户端只会看到一条连续的 SSE，
模型名、chunk id 全都是它以为的那个。

把 `serve` 换成 `mcp` 就变成 MCP Server，暴露 `gateway_chat` / `gateway_status` 两个工具
（Claude Desktop / Cursor 里直接配 `command: node, args: [dist/src/index.js, mcp]`）。

## 配置说明

```jsonc
{
  "listen": { "host": "127.0.0.1", "port": 8787 },
  "defaultModel": "gpt-5-codex",
  "aliases": { "gpt-4o": "gpt-5-codex" },      // 客户端模型名 → 路由键（灰度/换指向用）

  "limits": {
    "requestTimeoutMs": 60000,   // 单次尝试的总时长
    "firstTokenTimeoutMs": 20000,// 首字超时（超了就当这家挂了，换家）
    "idleTimeoutMs": 30000,      // 已提交后，多久不吐字算断流
    "totalTimeoutMs": 300000,    // 整条请求（含所有重试）的天花板
    "commitMode": "instant",     // ★ instant（默认，逐字下发）| buffered（攒批，可静默重放）
    "commitChars": 24,           // 仅 buffered：攒够多少个 code point 才提交
    "commitDelayMs": 400,        // 仅 buffered：或者等这么久就 flush（不 abort 上游）
    "emitRouteComments": false,  // true = 往 SSE 里写 : gateway provider=… 注释行
    "maxBodyBytes": 8388608
  },

  "policy": {
    "maxAttempts": 4,            // 一条链最多试几家
    "abortOnBadRequest": true,   // 400/参数错直接终止，不浪费备用额度
    "salvageOnStreamBreak": true,// 提交后断流是否续写
    "salvageInstruction": "…"    // 续写指令（中文/英文都行）
  },

  "models": {
    "gpt-5-codex": {
      "strategy": "priority",    // priority | round_robin
      "targets": [
        { "id": "codex-main",     "adapter": "codex",     "baseUrl": "https://api.openai.com/v1",          "apiKey": "${CODEX_API_KEY}",     "model": "gpt-5-codex" },
        { "id": "deepseek-backup","adapter": "openai",    "baseUrl": "https://api.deepseek.com/v1",        "apiKey": "${DEEPSEEK_API_KEY}",  "model": "deepseek-chat" },
        { "id": "glm-backup",     "adapter": "openai",    "baseUrl": "https://open.bigmodel.cn/api/paas/v4","apiKey": "${GLM_API_KEY}",      "model": "glm-4.6" },
        { "id": "claude-backup",  "adapter": "anthropic", "baseUrl": "https://api.anthropic.com/v1",       "apiKey": "${ANTHROPIC_API_KEY}","model": "claude-sonnet-4-5",
          "headers": { "anthropic-version": "2023-06-01" } }
      ]
    }
  }
}
```

target 上还可选：`enabled: false`（临时摘掉一家，不用删配置）、`timeoutMs`、`headers`、`weight`、`tags`、
`extraBody`（透传给该厂商的额外字段）。

旧键 `limits.commitBytes` 仍能加载（按 UTF-16 长度计，加载时 stderr 警告一次），但请迁到 `commitChars`。

### 管理接口与跨域

`POST /admin/reload` 不是公开端点：

- 配了 `AI_GATEWAY_ADMIN_TOKEN` → 必须 `Authorization: Bearer <token>`（先各自 sha256 再常量时间比较，
  连 token 长度都不泄露），失败 401；
- 没配 → 退化为"**仅回环地址** + 必须带自定义头 `x-gateway-admin: 1`"，否则 403。
  自定义头就是这道防线的关键：浏览器发简单请求（form / `<img>` / 裸 fetch）**无法**附带自定义头，
  跨站页面想触发 reload 必须先过 CORS 预检，而过不了。

`OPTIONS` 不再回 `access-control-allow-origin: *`。需要跨域时用 `AI_GATEWAY_CORS_ORIGINS`
（逗号分隔白名单）显式声明，未声明的 Origin 一律拿不到 ACAO 头。

### MCP 协议版本

`initialize` 只回**本服务端支持**的版本（`2025-06-18` / `2025-03-26` / `2024-11-05`）。客户端要的版本不在
列表里时，回自己的最高版本并在 `instructions` 里说明，由客户端决定是否继续 —— 绝不原样回显一个没实现过的版本号。

## 验证

```bash
pnpm test     # 46 个用例，零网络、零真实 key
```

覆盖：失败分类（429 额度 vs 限流、400 上下文 vs 参数错）、retry-after 三种写法、SSE 跨 chunk 分帧、
三种适配器的截断检测、透明切换、提交后 salvage 续写（含"续写请求里确实带上了半截文本"的断言）、
`bad_request` 终止整链、maxAttempts、冷却跳过、热重载成功/失败回滚/BOM、
**慢流不被误判断流**（`pacedSse` 3 字 / 600ms 间隔）、`instant` 首字不攒批、`buffered` 模式下同样不误杀、
SseWriter 背压与帧序、默认不泄露路由注释、`/admin/reload` 鉴权、MCP 协议版本协商、MCP stdin EOF 退出。

真实 socket 对照（起真上游 + 真网关子进程，全程不 mock）：

```bash
node scripts/probe-pacing.mjs
```

端到端演示（本地假上游，不需要任何 key）：

```bash
node scripts/fake-upstream.mjs primary 8790 &   # 永远 429
node scripts/fake-upstream.mjs backup  8791 &   # 正常流
AI_GATEWAY_CONFIG=./smoke.gateway.json node dist/src/index.js serve
curl -N http://127.0.0.1:8792/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

实测输出（配置里把 `limits.emitRouteComments` 打开才看得到注释行；默认关闭）：

```
: gateway provider=primary model=primary-model attempt=1
: gateway provider=backup model=backup-model attempt=2
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant"}}]}
data: {"id":"chatcmpl-…","choices":[{"index":0,"delta":{"content":"备"}}]}
…
data: [DONE]
```

服务端日志同一时刻：

```json
{"level":"warn","msg":"failover transparent","from":"primary","reason":"rate_limited","phase":"request","attempts":1}
{"level":"info","msg":"stream served","provider":"backup","attempts":2,"emittedChars":61}
```

## 设计上的取舍（以及后续可做的）

- **软提交的代价（按模式分）**：
  - `instant`（默认）：**没有首字代价**，逐字下发；代价是失去"静默重放"——第一帧发出去就提交了，
    之后上游失败只能走 salvage 续写。
  - `buffered`：保住零感知重放，代价是首字延迟上界 **`commitChars × 上游每字符间隔`**
    （20ms/token 时 ≈0.5s，40ms/token 时 ≈1s，与上游速率成正比；`commitDelayMs` 只是兜底，不是上界）。
  档位用 `commitMode` 选，不是靠把两个数字调小来"折中"——那只会让两条路都变差。
- **salvage 只能救文本**。如果断在 tool_call 中间，续写的 `tool_call_id` 可能与已发出的重复，
  当前策略是续写请求不带工具调用（`withContinuation` 只传 messages），让备用模型用纯文本收尾。
- **冷却表是进程内内存**：多副本部署时每个副本各自冷却。要全局一致，把 `ProviderHealth` 换成 Redis 实现即可
  （接口只有 5 个方法）。
- **未做**：token 计费统计、按租户配额、请求级 trace_id 透传、按 weight 的加权调度、
  `usage` 在续写场景下的跨供应商合并（当前以最后一次尝试为准）。
- MCP 侧目前是请求串行执行：并发打同一家供应商只会把 429 概率打满，串行+冷却反而更快。
