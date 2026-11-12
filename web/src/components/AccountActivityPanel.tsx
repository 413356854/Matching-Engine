import { useMemo, useState } from 'react';
import type { FundsFlow, OrderStatus, OrderView, TradingPair } from '../api';
import { formatDateTime, formatPrice, formatQuantity, formatUnits } from '../format';

interface AccountActivityPanelProps {
  pairs: TradingPair[];
  orders: OrderView[];
  ordersError: string | null;
  flows: FundsFlow[];
  flowsError: string | null;
  currencyScales: Record<string, number>;
}

type ActivityTab = 'orders' | 'flows';

const STATUS_LABEL: Record<OrderStatus, string> = {
  ACCEPTED: '等待冻结',
  PAID: '撮合中',
  PARTIALLY_FILLED: '部分成交',
  FILLED: '已成交',
  CANCELED: '已撤销',
  REJECTED: '已拒绝',
};

const STATUS_CLASS: Record<OrderStatus, string> = {
  ACCEPTED: 'pending',
  PAID: 'live',
  PARTIALLY_FILLED: 'partial',
  FILLED: 'done',
  CANCELED: 'flat',
  REJECTED: 'down',
};

function flowDirection(op: string): 'up' | 'down' | 'flat' {
  if (op === 'AVAILABLE_CREDIT' || op === 'FROZEN_CREDIT') {
    return 'up';
  }
  if (op === 'AVAILABLE_DEBIT' || op === 'FROZEN_DEBIT') {
    return 'down';
  }
  return 'flat';
}

function flowSign(op: string): string {
  const direction = flowDirection(op);
  return direction === 'up' ? '+' : direction === 'down' ? '−' : '';
}

/**
 * 后端 logDesc 形如「下单冻结（可用转冻结）」；交易页面只展示业务日志类型，
 * 不展示括号里的原子操作类型。
 */
function logTypeLabel(logDesc: string): string {
  return logDesc.replace(/（[^）]*）/g, '').trim();
}

export function AccountActivityPanel({
  pairs,
  orders,
  ordersError,
  flows,
  flowsError,
  currencyScales,
}: AccountActivityPanelProps) {
  const [tab, setTab] = useState<ActivityTab>('orders');

  const pairMap = useMemo(
    () => new Map(pairs.map((item) => [item.symbol, item])),
    [pairs],
  );

  const count = tab === 'orders' ? orders.length : flows.length;
  const error = tab === 'orders' ? ordersError : flowsError;

  return (
    <section className="panel open-orders-panel">
      <div className="panel-header book-trades-header">
        <div className="book-trades-tabs" role="tablist" aria-label="当前委托与历史流水">
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'orders'}
            className={`book-trades-tab ${tab === 'orders' ? 'active' : ''}`}
            onClick={() => setTab('orders')}
          >
            当前委托
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'flows'}
            className={`book-trades-tab ${tab === 'flows' ? 'active' : ''}`}
            onClick={() => setTab('flows')}
          >
            历史流水
          </button>
        </div>

        <div className="open-orders-meta">
          <span className="orders-count">{count}</span>
          <span className="refresh-badge">
            <span className="live-dot" />
            {tab === 'orders' ? '3s' : '5s'}
          </span>
        </div>
      </div>

      <div className="orders-scroll">
        {tab === 'orders' ? (
          <table className="orders-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>交易对</th>
                <th>方向</th>
                <th className="align-right">价格</th>
                <th className="align-right">数量</th>
                <th className="align-right">已成交</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((order) => {
                const orderPair = pairMap.get(order.symbol);
                const priceScale = orderPair?.priceScale ?? 0;
                const quantityScale = orderPair?.quantityScale ?? 0;
                const side = order.side === 'BUY' ? 'up' : 'down';
                return (
                  <tr
                    key={order.orderId}
                    className="order-row"
                    title={`orderId: ${order.orderId} · ${formatDateTime(order.createdAt)}`}
                  >
                    <td className="mono muted">{formatDateTime(order.createdAt)}</td>
                    <td>{order.symbol}</td>
                    <td>
                      <span className={`side-pill ${side}`}>
                        {order.side === 'BUY' ? '买入' : '卖出'}
                      </span>
                    </td>
                    <td className="mono align-right">
                      {order.type === 'MARKET'
                        ? '市价'
                        : formatPrice(order.price, priceScale)}
                    </td>
                    <td className="mono align-right">
                      {formatQuantity(order.quantity, quantityScale)}
                    </td>
                    <td className="mono align-right">
                      {formatQuantity(order.filledQuantity, quantityScale)}
                    </td>
                    <td>
                      <span className={`order-status ${STATUS_CLASS[order.status]}`}>
                        {STATUS_LABEL[order.status]}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <table className="orders-table flow-history-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>币种</th>
                <th>日志类型</th>
                <th className="align-right">金额</th>
                <th className="align-right">可用余额</th>
                <th className="align-right">冻结余额</th>
              </tr>
            </thead>
            <tbody>
              {flows.map((flow) => {
                const scale = currencyScales[flow.currency] ?? 0;
                const direction = flowDirection(flow.op);
                return (
                  <tr
                    key={`${flow.eventId}-${flow.seq}-${flow.occurredAt}`}
                    className="order-row"
                    title={`eventId: ${flow.eventId}\nrequestId: ${flow.requestId}\n${flow.logType} · seq ${flow.seq}`}
                  >
                    <td className="mono muted">{formatDateTime(flow.occurredAt)}</td>
                    <td>{flow.currency}</td>
                    <td>
                      <span className="admin-type">{logTypeLabel(flow.logDesc)}</span>
                    </td>
                    <td className={`mono align-right flow-amount ${direction}`}>
                      {flowSign(flow.op)}
                      {formatUnits(flow.amount, scale)}
                    </td>
                    <td
                      className="mono align-right"
                      title={`可用 ${formatUnits(flow.availableBefore, scale)} → ${formatUnits(
                        flow.availableAfter,
                        scale,
                      )}`}
                    >
                      {formatUnits(flow.availableAfter, scale)}
                    </td>
                    <td
                      className="mono align-right"
                      title={`冻结 ${formatUnits(flow.frozenBefore, scale)} → ${formatUnits(
                        flow.frozenAfter,
                        scale,
                      )}`}
                    >
                      {formatUnits(flow.frozenAfter, scale)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}

        {count === 0 && (
          <div className="empty-hint orders-empty">
            {error
              ? error
              : tab === 'orders'
                ? '暂无当前委托。下单成功后会自动出现在这里。'
                : '暂无历史流水。充值、冻结或成交产生的流水会异步归档到这里。'}
          </div>
        )}
      </div>
    </section>
  );
}
