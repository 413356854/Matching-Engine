import type { Trade, TradingPair } from '../api';
import { formatPrice, formatQuantity, formatTime } from '../format';

interface LatestTradesPanelProps {
  pair?: TradingPair;
  trades: Trade[];
  error: string | null;
  /** 嵌在共享栏目里时只渲染内容，不渲染独立面板头和边框。 */
  embedded?: boolean;
}

export function LatestTradesPanel({
  pair,
  trades,
  error,
  embedded = false,
}: LatestTradesPanelProps) {
  const content = (
    <>
      <div className="compact-trade-columns">
        <span>价格</span>
        <span className="align-right">数量</span>
        <span className="align-right">时间</span>
      </div>

      <div className="compact-trade-scroll">
        {trades.map((trade) => {
          const side =
            trade.takerSide === 'BUY' ? 'up' : trade.takerSide === 'SELL' ? 'down' : 'flat';
          return (
            <div
              key={`${trade.tradeId}-${trade.executedAt}`}
              className="compact-trade-row"
              title={
                trade.takerSide === 'BUY'
                  ? '买入'
                  : trade.takerSide === 'SELL'
                    ? '卖出'
                    : '未知方向'
              }
            >
              <span className={`mono ${side}`}>
                {pair ? formatPrice(trade.price, pair.priceScale) : trade.price}
              </span>
              <span className="mono align-right muted">
                {pair ? formatQuantity(trade.quantity, pair.quantityScale) : trade.quantity}
              </span>
              <span className="mono align-right muted">{formatTime(trade.executedAt)}</span>
            </div>
          );
        })}

        {trades.length === 0 && (
          <div className="empty-hint compact-trade-empty">
            {error ? error : '暂无成交，撮合完成后会实时推送逐笔成交。'}
          </div>
        )}
      </div>
    </>
  );

  if (embedded) {
    return content;
  }

  return (
    <section className="panel latest-trades-panel">
      <div className="panel-header latest-trades-header">
        <div>
          <h2>最新成交</h2>
          <span className="panel-sub">
            {pair ? `${pair.baseCurrency}/${pair.quoteCurrency}` : '实时逐笔'}
          </span>
        </div>
        <span className="refresh-badge">
          <span className="live-dot" />
          3s
        </span>
      </div>
      {content}
    </section>
  );
}
