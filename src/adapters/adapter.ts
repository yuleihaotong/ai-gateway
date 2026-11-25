/**
 * 模型适配器契约。
 *
 * 这一层是"平移到新模型"的唯一改动点。要接一家新厂商，实现四个方法即可：
 *   build        —— IR → 该厂商的 HTTP 请求
 *   decodeJson   —— 非流式响应 → IR
 *   decodeStream —— SSE 帧 → IR 事件流（★ 必须能识别"流被掐断"）
 *   decodeError  —— HTTP 错误 → 统一失败（默认实现已覆盖 90% 情况）
 */
import type { CanonRequest, CanonResponse, StreamEvent } from '../protocol/types.js';
import type { SseFrame } from '../protocol/sse.js';
import type { TargetSpec } from '../core/target.js';
import { classifyHttpFailure, classifyThrown } from '../core/classifier.js';
import { UpstreamFailure } from '../core/errors.js';

export interface BuiltRequest {
  url: string;
  headers: Record<string, string>;
  body: string;
}

export interface Adapter {
  readonly kind: string;
  build(target: TargetSpec, req: CanonRequest, stream: boolean): BuiltRequest;
  decodeJson(target: TargetSpec, json: unknown): CanonResponse;
  decodeStream(target: TargetSpec, frames: AsyncGenerator<SseFrame>): AsyncGenerator<StreamEvent>;
  decodeError(target: TargetSpec, status: number, headers: Headers, bodyText: string): UpstreamFailure;
}

export function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

export abstract class BaseAdapter implements Adapter {
  abstract readonly kind: string;
  abstract build(target: TargetSpec, req: CanonRequest, stream: boolean): BuiltRequest;
  abstract decodeJson(target: TargetSpec, json: unknown): CanonResponse;
  abstract decodeStream(target: TargetSpec, frames: AsyncGenerator<SseFrame>): AsyncGenerator<StreamEvent>;

  decodeError(target: TargetSpec, status: number, headers: Headers, bodyText: string): UpstreamFailure {
    return classifyHttpFailure(target.id, status, headers, bodyText);
  }

  protected wrapThrown(target: TargetSpec, err: unknown): UpstreamFailure {
    return err instanceof UpstreamFailure ? err : classifyThrown(target.id, err);
  }
}
