/** 结构化日志：一行一条 JSON，方便被 filebeat / loki 直接吃掉。 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const RANK: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export interface LoggerOptions {
  /** 默认 stdout。MCP stdio 模式下必须换成 stderr，否则会污染 JSON-RPC 通道。 */
  out?: { write(chunk: string): unknown };
  err?: { write(chunk: string): unknown };
}

export class Logger {
  constructor(
    private readonly level: LogLevel = 'info',
    private readonly scope = 'gateway',
    private readonly options: LoggerOptions = {},
  ) {}

  child(scope: string): Logger {
    return new Logger(this.level, `${this.scope}:${scope}`, this.options);
  }

  debug(msg: string, fields?: Record<string, unknown>): void {
    this.emit('debug', msg, fields);
  }
  info(msg: string, fields?: Record<string, unknown>): void {
    this.emit('info', msg, fields);
  }
  warn(msg: string, fields?: Record<string, unknown>): void {
    this.emit('warn', msg, fields);
  }
  error(msg: string, fields?: Record<string, unknown>): void {
    this.emit('error', msg, fields);
  }

  private emit(level: LogLevel, msg: string, fields?: Record<string, unknown>): void {
    if (RANK[level] < RANK[this.level]) return;
    const line = JSON.stringify({ ts: new Date().toISOString(), level, scope: this.scope, msg, ...fields }) + '\n';
    const isErr = level === 'error' || level === 'warn';
    const sink = isErr ? (this.options.err ?? process.stderr) : (this.options.out ?? process.stdout);
    sink.write(line);
  }
}
