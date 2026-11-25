import { test } from 'node:test';
import assert from 'node:assert/strict';

import { readSseFrames, SseWriter, type SseFrame } from '../src/protocol/sse.js';

const enc = new TextEncoder();

function streamOf(pieces: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const piece of pieces) controller.enqueue(enc.encode(piece));
      controller.close();
    },
  });
}

async function framesOf(pieces: string[]): Promise<SseFrame[]> {
  const out: SseFrame[] = [];
  for await (const frame of readSseFrames(streamOf(pieces))) out.push(frame);
  return out;
}

test('分帧器能跨 chunk 边界拼出完整帧', async () => {
  const frames = await framesOf(['da', 'ta: hel', 'lo\n', '\n']);
  assert.equal(frames.length, 1);
  assert.equal(frames[0]?.data, 'hello');
});

test('CRLF、event 名、多行 data、注释行都要能处理', async () => {
  const frames = await framesOf([': keepalive\r\n', 'event: ping\r\ndata: a\r\ndata: b\r\n\r\n']);
  assert.equal(frames.length, 1);
  assert.equal(frames[0]?.event, 'ping');
  assert.equal(frames[0]?.data, 'a\nb');
});

test('EOF 处没有终止空行的残帧也要吐出来（完整性判定交给 Adapter）', async () => {
  const frames = await framesOf(['data: tail']);
  assert.equal(frames.length, 1);
  assert.equal(frames[0]?.data, 'tail');
});

test('SseWriter 输出 OpenAI 客户端认识的帧格式', async () => {
  const out: string[] = [];
  const writer = new SseWriter({
    write: (chunk) => {
      out.push(chunk);
      return true;
    },
    end: () => undefined,
  });

  writer.comment('gateway provider=b');
  writer.frame('{"a":1}');
  writer.json({ b: 2 }, 'custom');
  writer.done();

  // write 现在是背压感知的 promise 链（write=false 要等 drain），
  // 断言点不变，只是必须先 await close() 把队列排空。
  await writer.close();

  const text = out.join('');
  assert.match(text, /^: gateway provider=b\n\n/);
  assert.match(text, /data: \{"a":1\}\n\n/);
  assert.match(text, /event: custom\ndata: \{"b":2\}\n\n/);
  assert.match(text, /data: \[DONE\]\n\n$/);
});
