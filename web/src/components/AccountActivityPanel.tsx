import { useMemo, useState } from 'react';
import {
  ApiError,
  cancelOrder,
  type FundsFlow,
  type OrderHistory,
  type OrderStatus,
  type OrderView,
  type TradingPair,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime, formatPrice, formatQuantity, formatUnits } from '../format';
import type { ToastKind } from './ToastStack';

interface AccountActivityPanelProps {
  pairs: TradingPair[];
  orders: OrderView[];
  ordersError: string | null;
  history: OrderHistory[];
  historyError: string | null;
  flows: FundsFlow[];
  flowsError: string | null;
  currencyScales: Record<string, number>;
  userId: number;
  onOrderCanceled: () => void;
  notify: (kind: ToastKind, text: string) => void;
}

type ActivityTab = 'orders' | 'history' | 'flows';

/** 委托历史只包含两种终态，文案与「当前委托」状态机区分：FILLED 显示为撮合完成。 */
const HISTORY_STATUS_LABEL: Partial<Record<OrderStatus, string>> = {
  FILLED: '撮合完成',
  CANCELED: '已撤销',
};

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
  history,
  historyError,
  flows,
  flowsError,
  currencyScales,
  userId,
  onOrderCanceled,
  notify,
}: AccountActivityPanelProps) {
  const [tab, setTab] = useState<ActivityTab>('orders');
  const [cancelingOrderId, setCancelingOrderId] = useState<number | null>(null);

  const pairMap = useMemo(
    () => new Map(pairs.map((item) => [item.symbol, item])),
    [pairs],
  );

  // 与后端可撤状态一致：已冻结进入撮合（PAID）或部分成交的委托可以撤单；
  // ACCEPTED 仍在冻结回执途中，点击后由后端返回 ORDER_NOT_PAID_YET，前端先禁用。
  const cancelable = (order: OrderView) =>
    order.status === 'PAID' || order.status === 'PARTIALLY_FILLED';

  const handleCancel = async (order: OrderView) => {
    if (cancelingOrderId !== null) {
      return;
    }
    setCancelingOrderId(order.orderId);
    try {
      const response = await cancelOrder({
        requestId: crypto.randomUUID(),
        userId,
        symbol: order.symbol,
        orderId: order.orderId,
      });
      if (!response.success) {
        throw new ApiError(response.reason ?? 'CANCEL_REJECTED', 400, response);
      }
      notify('success', `撤单指令已提交：#${order.orderId}`);
      onOrderCanceled();
    } catch (error) {
      notify('error', describeError(error));
    } finally {
      setCancelingOrderId(null);
    }
  };

  const count = tab === 'orders' ? orders.length : tab === 'history' ? history.length : flows.length;
  const error = tab === 'orders' ? ordersError : tab === 'history' ? historyError : flowsError;

  return (
    <section className="panel open-orders-panel">
      <div className="panel-header book-trades-header">
        <div className="book-trades-tabs" role="tablist" aria-label="当前委托、历史委托与历史流水">
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
            aria-selected={tab === 'history'}
            className={`book-trades-tab ${tab === 'history' ? 'active' : ''}`}
            onClick={() => setTab('history')}
          >
            历史委托
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
                <th className="align-right">操作</th>
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
                    <td className="align-right">
                      <button
                        type="button"
                        className="cancel-order-button"
                        disabled={!cancelable(order) || cancelingOrderId !== null}
                        onClick={() => void handleCancel(order)}
                        title={
                          cancelable(order)
                            ? '撤销该委托并解冻剩余资金'
                            : order.status === 'ACCEPTED'
                              ? '等待资金冻结完成，暂不能撤单'
                              : '当前状态不可撤单'
                        }
                      >
                        {cancelingOrderId === order.orderId ? '撤单中…' : '撤单'}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : tab === 'history' ? (
          <table className="orders-table">
            <thead>
              <tr>
                <th>成交/撤单时间</th>
                <th>交易对</th>
                <th>方向</th>
                <th className="align-right">价格</th>
                <th className="align-right">数量</th>
                <th className="align-right">已成交</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {history.map((order) => {
                const side = order.side === 'BUY' ? 'up' : 'down';
                return (
                  <tr
                    key={order.orderId}
                    className="order-row"
                    title={`orderId: ${order.orderId}\nrequestId: ${order.requestId}\n创建 ${formatDateTime(
                      order.createdAt,
                    )} · 终态 ${formatDateTime(order.finalAt)} · seq ${order.seq}`}
                  >
                    <td className="mono muted">{formatDateTime(order.finalAt)}</td>
                    <td>{order.symbol}</td>
                    <td>
                      <span className={`side-pill ${side}`}>
                        {order.side === 'BUY' ? '买入' : '卖出'}
                      </span>
                    </td>
                    <td className="mono align-right">
                      {order.type === 'MARKET'
                        ? '市价'
                        : formatPrice(order.price, order.priceScale)}
                    </td>
                    <td className="mono align-right">
                      {formatQuantity(order.quantity, order.quantityScale)}
                    </td>
                    <td className="mono align-right">
                      {formatQuantity(order.filledQuantity, order.quantityScale)}
                    </td>
                    <td>
                      <span className={`order-status ${STATUS_CLASS[order.status]}`}>
                        {HISTORY_STATUS_LABEL[order.status] ?? STATUS_LABEL[order.status]}
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
                : tab === 'history'
                  ? '暂无历史委托。撤单或撮合完成的委托会异步归档到这里。'
                  : '暂无历史流水。充值、冻结或成交产生的流水会异步归档到这里。'}
          </div>
        )}
      </div>
    </section>
  );
}
