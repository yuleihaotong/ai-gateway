/**
 * 本地假上游，用来端到端验证降级链路（不需要任何真实 API Key）。
 *
 *   node scripts/fake-upstream.mjs primary 8790   # 永远返回 429，扮演"被打爆的主模型"
 *   node scripts/fake-upstream.mjs backup  8791   # 正常流式输出，扮演备用模型
 *
 * 配合 `node dist/src/index.js serve` 即可看到：客户端请求主模型 → 网关静默切到备用 →
 * 客户端只看到一段完整、连续的 SSE。
 */
import { createServer } from 'node:http';

const mode = process.argv[2] ?? 'backup';
const port = Number(process.argv[3] ?? (mode === 'primary' ? 8790 : 8791));

const BACKUP_TEXT = '备用模型已接管：主模型 429 之后，这段内容由降级链上的第二个供应商流式产出。';
const STREAM_TAIL = '以上内容在客户端看来与主模型输出完全一致。';

function sseHead(res) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
}

const server = createServer(async (req, res) => {
  let raw = '';
  for await (const piece of req) raw += piece;
  let body = {};
  try {
    body = JSON.parse(raw || '{}');
  } catch {
    body = {};
  }
  const model = typeof body.model === 'string' ? body.model : 'unknown';
  const stream = body.stream === true;

  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, mode }));
    return;
  }

  if (mode === 'primary') {
    res.writeHead(429, { 'content-type': 'application/json', 'retry-after': '30' });
    res.end(JSON.stringify({ error: { message: 'Rate limit reached for requests (primary is hot)', type: 'rate_limit_error' } }));
    return;
  }

  if (!stream) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({
        id: 'chatcmpl-fake',
        model,
        choices: [{ index: 0, message: { role: 'assistant', content: BACKUP_TEXT }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 12, completion_tokens: BACKUP_TEXT.length },
      }),
    );
    return;
  }

  sseHead(res);
  const send = (payload) => res.write(`data: ${JSON.stringify(payload)}\n\n`);
  send({ id: 'chatcmpl-fake', model, choices: [{ index: 0, delta: { role: 'assistant' } }] });
  for (const char of BACKUP_TEXT + STREAM_TAIL) {
    send({ id: 'chatcmpl-fake', model, choices: [{ index: 0, delta: { content: char } }] });
  }
  send({
    id: 'chatcmpl-fake',
    model,
    choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    usage: { prompt_tokens: 12, completion_tokens: (BACKUP_TEXT + STREAM_TAIL).length },
  });
  res.write('data: [DONE]\n\n');
  res.end();
});

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(`fake upstream [${mode}] listening on http://127.0.0.1:${port}\n`);
});
