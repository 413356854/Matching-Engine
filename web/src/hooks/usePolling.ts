import { useEffect, useRef } from 'react';

export type PollCallback = (signal: AbortSignal) => Promise<void> | void;

/**
 * 立即执行一次；传入间隔时按固定间隔轮询，传 null 时只在依赖变化时执行一次（手动刷新场景）。
 * 组件卸载或依赖变化时中止进行中的请求并清掉定时器。
 * 回调里错误的处理交给调用方。
 */
export function usePolling(
  callback: PollCallback,
  intervalMs: number | null,
  deps: ReadonlyArray<unknown>,
): void {
  const callbackRef = useRef(callback);

  useEffect(() => {
    callbackRef.current = callback;
  });

  useEffect(() => {
    let disposed = false;
    let running = false;
    let controller = new AbortController();

    const tick = async () => {
      if (disposed || running) {
        return;
      }
      running = true;
      try {
        await callbackRef.current(controller.signal);
      } catch {
        // 调用方自行记录错误；这里只保证轮询不中断。
      } finally {
        running = false;
      }
    };

    void tick();
    const timer = intervalMs === null ? null : window.setInterval(tick, intervalMs);

    return () => {
      disposed = true;
      if (timer !== null) {
        window.clearInterval(timer);
      }
      controller.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, intervalMs]);
}
