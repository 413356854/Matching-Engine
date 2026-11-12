import { useEffect, useRef } from 'react';
import type { DepthLevel, OrderBookDepth, Trade, TradingPair } from '../api';
import { formatPrice, formatQuantity } from '../format';

interface OrderBookPanelProps {
  pair?: TradingPair;
  orderBook: OrderBookDepth | null;
  /** 最新一笔成交（trades 按时间倒序，传第一条）；盘口中间展示它的成交价。 */
  lastTrade?: Trade;
  error: string | null;
  /** 嵌在共享栏目里时只渲染内容，不渲染独立面板头和边框。 */
  embedded?: boolean;
}

interface DepthRow extends DepthLevel {
  cumulative: number;
}

function withCumulative(levels: DepthLevel[]): { rows: DepthRow[]; total: number } {
  let total = 0;
  const rows = levels.map((level) => {
    total += level.quantity;
    return { ...level, cumulative: total };
  });
  return { rows, total };
}

export function OrderBookPanel({
  pair,
  orderBook,
  lastTrade,
  error,
  embedded = false,
}: OrderBookPanelProps) {
  const priceScale = pair?.priceScale ?? 0;
  const quantityScale = pair?.quantityScale ?? 0;
  const askScrollRef = useRef<HTMLDivElement>(null);

  // 后端返回的卖盘是价格升序（卖一在前），展示时翻转成高价在上；买盘本身降序（买一在前）。
  const askData = withCumulative(orderBook?.asks ?? []);
  const askRows = [...askData.rows].reverse();
  const bidData = withCumulative(orderBook?.bids ?? []);

  // 卖盘较多时默认滚到最底部，让卖一紧贴中间的价差行。
  useEffect(() => {
    const element = askScrollRef.current;
    if (element) {
      element.scrollTop = element.scrollHeight;
    }
  }, [askRows.length, pair?.symbol]);

  const maxCumulative = Math.max(askData.total, bidData.total, 1);
  const hasOrders = askRows.length > 0 || bidData.rows.length > 0;
  const bestBid = orderBook?.bestBid ?? null;
  const bestAsk = orderBook?.bestAsk ?? null;
  const spread = bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null;
  const marketNote =
    orderBook && (orderBook.marketBidCount > 0 || orderBook.marketAskCount > 0)
      ? `市价挂单：买 ${orderBook.marketBidCount} / 卖 ${orderBook.marketAskCount}`
      : null;

  const barWidth = (cumulative: number) =>
    `${Math.max(2, Math.min(100, (cumulative / maxCumulative) * 100))}%`;

  const content = (
    <>
      <div className="ob-columns">
        <span>价格{pair ? ` (${pair.quoteCurrency})` : ''}</span>
        <span className="align-right">数量{pair ? ` (${pair.baseCurrency})` : ''}</span>
      </div>

      <div className="ob-scroll">
        <div className="ob-side ob-asks" ref={askScrollRef}>
          <div className="ob-side-inner">
            {askRows.map((row) => (
              <div
                key={`ask-${row.price}`}
                className="ob-row ask"
                title={`${row.orderCount} 笔挂单，累计 ${formatQuantity(row.cumulative, quantityScale)}`}
              >
                <span className="ob-bar" style={{ width: barWidth(row.cumulative) }} />
                <span className="ob-price">{formatPrice(row.price, priceScale)}</span>
                <span className="ob-qty">{formatQuantity(row.quantity, quantityScale)}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="ob-spread ob-last-trade">
          <span>最新成交价</span>
          <b
            className={
              lastTrade?.takerSide === 'BUY'
                ? 'up'
                : lastTrade?.takerSide === 'SELL'
                  ? 'down'
                  : undefined
            }
          >
            {lastTrade && lastTrade.price > 0
              ? formatPrice(lastTrade.price, priceScale)
              : '--'}
          </b>
        </div>

        <div className="ob-side ob-bids">
          <div className="ob-side-inner">
            {bidData.rows.map((row) => (
              <div
                key={`bid-${row.price}`}
                className="ob-row bid"
                title={`${row.orderCount} 笔挂单，累计 ${formatQuantity(row.cumulative, quantityScale)}`}
              >
                <span className="ob-bar" style={{ width: barWidth(row.cumulative) }} />
                <span className="ob-price">{formatPrice(row.price, priceScale)}</span>
                <span className="ob-qty">{formatQuantity(row.quantity, quantityScale)}</span>
              </div>
            ))}

            {!hasOrders && (
              <div className="empty-hint ob-empty">
                {error
                  ? error
                  : orderBook
                    ? '暂无挂单，撮合后会实时展示深度。'
                    : '正在加载订单簿…'}
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="ob-footer">
        <span>价差 {spread !== null ? formatPrice(spread, priceScale) : '--'}</span>
        {marketNote && <span>{marketNote}</span>}
      </div>
    </>
  );

  if (embedded) {
    return content;
  }

  return (
    <section className="panel orderbook-panel">
      <div className="panel-header orderbook-header">
        <div>
          <h2>订单簿</h2>
          <span className="panel-sub">
            {pair ? `${pair.baseCurrency}/${pair.quoteCurrency}` : '实时深度'} · 限价单
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
