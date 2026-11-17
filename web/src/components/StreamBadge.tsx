import type { OrderBookSocketStatus } from '../hooks/useOrderBookSocket';

interface StreamBadgeProps {
  status: OrderBookSocketStatus;
  /** 已连接时展示的推送周期；后端默认 500ms。 */
  intervalLabel?: string;
}

/** 盘口推送状态徽标：绿色=已连接，黄色=连接中，红色=断开重连中。 */
export function StreamBadge({ status, intervalLabel = 'WS 500ms' }: StreamBadgeProps) {
  if (status === 'open') {
    return (
      <span className="refresh-badge" title="盘口通过 WebSocket 实时推送">
        <span className="live-dot" />
        {intervalLabel}
      </span>
    );
  }
  if (status === 'connecting') {
    return (
      <span className="refresh-badge" title="正在建立盘口推送连接">
        <span className="live-dot pending" />
        连接中
      </span>
    );
  }
  return (
    <span className="refresh-badge" title="盘口推送已断开，正在自动重连">
      <span className="live-dot offline" />
      重连中
    </span>
  );
}
