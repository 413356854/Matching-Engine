import type { BotStatus } from '../api';

interface BotTradingButtonProps {
  status: BotStatus | null;
  busy: boolean;
  onToggle: () => void;
}

/**
 * 机器人自动交易开关：绿色=运行且币安行情已连接，琥珀=等待行情，灰色=已停止。
 * 鼠标悬停展示机器人用户、下单间隔、币安最新价与下单统计。
 */
export function BotTradingButton({ status, busy, onToggle }: BotTradingButtonProps) {
  if (!status) {
    return null;
  }

  const tone = status.trading ? (status.binanceConnected ? 'on' : 'waiting') : 'off';
  const label = busy ? '处理中…' : status.trading ? '关闭自动交易' : '开启自动交易';
  const title = [
    `机器人用户 ${status.userId}`,
    `下单间隔 ${status.orderIntervalMs}ms`,
    `币安行情：${status.binanceConnected ? '已连接' : '未连接'}`,
    `网络：${status.proxy}`,
    ...status.quotes.map((quote) => `${quote.symbol} ${quote.price}`),
    `已下单 ${status.ordersPlaced}，同步拒绝 ${status.ordersRejected}`,
    status.lastError ? `最近错误：${status.lastError}` : null,
    status.binanceError ? `行情错误：${status.binanceError}` : null,
    status.enabled ? null : '机器人模块未启用（app.bot.enabled=false）',
  ]
    .filter(Boolean)
    .join('\n');

  return (
    <button
      type="button"
      className={`bot-button ${tone}`}
      disabled={busy || !status.enabled}
      title={title}
      onClick={onToggle}
    >
      <i aria-hidden="true" />
      {label}
    </button>
  );
}
