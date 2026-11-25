/**
 * SSE 编解码。
 *
 * 自己实现而不是用 eventsource-parser，是因为这里有两个硬要求：
 *  1) 分片必须容忍任意位置截断（网络 chunk 不保证按帧边界到达）；
 *  2) 必须能区分"正常结束"和"流被掐断" —— 这决定了要不要触发降级。
 */

export interface SseFrame {
  event?: string;
  data: string;
  id?: string;
  retry?: number;
}

interface Boundary {
  index: number;
  length: number;
}

function findBoundary(buf: string): Boundary | null {
  const lf = buf.indexOf('\n\n');
  const crlf = buf.indexOf('\r\n\r\n');
  const cr = buf.indexOf('\r\r');
  const candidates: Boundary[] = [];
  if (lf >= 0) candidates.push({ index: lf, length: 2 });
  if (crlf >= 0) candidates.push({ index: crlf, length: 4 });
  if (cr >= 0) candidates.push({ index: cr, length: 2 });
  if (candidates.length === 0) return null;
  return candidates.reduce((a, b) => (a.index <= b.index ? a : b));
}

function parseBlock(raw: string): SseFrame | null {
  const lines = raw.split(/\r\n|\n|\r/);
  let event: string | undefined;
  let id: string | undefined;
  let retry: number | undefined;
  const data: string[] = [];

  for (const line of lines) {
    if (!line) continue;
    if (line.startsWith(':')) continue; // keep-alive 注释
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    switch (field) {
      case 'event':
        event = value;
        break;
      case 'data':
        data.push(value);
        break;
      case 'id':
        id = value;
        break;
      case 'retry': {
        const n = Number(value);
        if (Number.isFinite(n)) retry = n;
        break;
      }
      default:
        break;
    }
  }

  if (data.length === 0 && event === undefined) return null;
  return { data: data.join('\n'), ...(event ? { event } : {}), ...(id ? { id } : {}), ...(retry ? { retry } : {}) };
}

/**
 * 增量读取 SSE 帧。
 * 注意：这里不会因为 EOF 就抛错 —— "有没有正常结束"由各 Adapter 依据业务字段判断
 * （OpenAI 看 [DONE] / finish_reason，Anthropic 看 message_stop）。
 */
export async function* readSseFrames(
  body: ReadableStream<Uint8Array>,
  opts: { signal?: AbortSignal } = {},
): AsyncGenerator<SseFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  // 客户端断开时不能干等下一块数据：直接 cancel 读端，让 read() 立刻以 done 返回，
  // 顺便把上游 socket 释放掉（否则剩余帧会被一路解码进内存）。
  const onAbort = (): void => {
    void reader.cancel(opts.signal?.reason).catch(() => undefined);
  };
  opts.signal?.addEventListener('abort', onAbort, { once: true });
  let buf = '';

  try {
    for (;;) {
      if (opts.signal?.aborted) {
        throw new DOMException('client aborted', 'AbortError');
      }
      const { value, done } = await reader.read();
      if (done) {
        if (opts.signal?.aborted) throw new DOMException('client aborted', 'AbortError');
        break;
      }
      buf += decoder.decode(value, { stream: true });

      for (;;) {
        const boundary = findBoundary(buf);
        if (!boundary) break;
        const rawBlock = buf.slice(0, boundary.index);
        buf = buf.slice(boundary.index + boundary.length);
        const frame = parseBlock(rawBlock);
        if (frame) yield frame;
      }
    }

    buf += decoder.decode();
    // 上游没发空行就关连接：最后半帧仍然尽量解析出来，交给 Adapter 判断完整性
    if (buf.trim().length > 0) {
      const frame = parseBlock(buf);
      if (frame) yield frame;
    }
  } finally {
    opts.signal?.removeEventListener('abort', onAbort);
    try {
      reader.releaseLock();
    } catch {
      /* already released */
    }
  }
}

export interface SseSink {
  write(chunk: string): boolean;
  end(): void;
  /** 可选：支持 `drain` 事件才算真正支持背压；没有就只能"写完算完"。 */
  once?(event: 'drain', listener: () => void): unknown;
}

/**
 * 把帧写回给调用方（OpenAI 兼容格式）。
 *
 * ★ 所有写操作串成一条 promise 链：`sink.write()` 返回 false 表示内核缓冲区已满，
 *   必须等 `drain` 才能继续。没有这条链，大响应会在内存里堆成一座山，
 *   而且帧序可能因为异步交错而乱掉。
 */
export class SseWriter {
  private closed = false;
  private tail: Promise<void> = Promise.resolve();
  private drainWait: Promise<void> | null = null;

  constructor(private readonly sink: SseSink) {}

  private enqueue(text: string): Promise<void> {
    this.tail = this.tail.then(async () => {
      if (this.closed) return;
      if (this.sink.write(text) === false) await this.waitDrain();
    });
    return this.tail;
  }

  private waitDrain(): Promise<void> {
    const once = this.sink.once;
    if (!once) return Promise.resolve();
    if (!this.drainWait) {
      this.drainWait = new Promise<void>((resolve) => {
        once.call(this.sink, 'drain', () => {
          this.drainWait = null;
          resolve();
        });
      });
    }
    return this.drainWait;
  }

  /** SSE 注释：客户端一律忽略。只有 limits.emitRouteComments 打开时才会写。 */
  comment(text: string): Promise<void> {
    if (this.closed) return Promise.resolve();
    return this.enqueue(`: ${text}\n\n`);
  }

  frame(data: string, event?: string): Promise<void> {
    if (this.closed) return Promise.resolve();
    let out = '';
    if (event) out += `event: ${event}\n`;
    // data 里的换行必须拆成多个 data: 行，否则客户端会把一帧切成两帧
    for (const line of data.split('\n')) out += `data: ${line}\n`;
    return this.enqueue(out + '\n');
  }

  json(payload: unknown, event?: string): Promise<void> {
    return this.frame(JSON.stringify(payload), event);
  }

  /** OpenAI 流结束哨兵 */
  done(): Promise<void> {
    return this.frame('[DONE]');
  }

  /** 等所有已入队的帧真正写出去，再结束响应 —— 否则最后几帧会被 end() 吞掉。 */
  async close(): Promise<void> {
    await this.tail;
    if (this.closed) return;
    this.closed = true;
    this.sink.end();
  }
}
