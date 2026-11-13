import { useEffect, useState, type FormEvent } from 'react';
import {
  fetchAdminOrderHistory,
  isAbortError,
  ORDER_HISTORY_STATUSES,
  type AdminOrderHistoryQuery,
  type OrderHistory,
  type OrderStatus,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime, formatPrice, formatQuantity } from '../format';
import { RefreshIcon } from './icons';

const DEFAULT_LIMIT = 100;
const LIMIT_OPTIONS = [50, 100, 200, 500, 1000];

const STATUS_LABEL: Partial<Record<OrderStatus, string>> = {
  FILLED: '撮合完成',
  CANCELED: '已撤销',
};

const STATUS_CLASS: Partial<Record<OrderStatus, string>> = {
  FILLED: 'done',
  CANCELED: 'flat',
};

function digitsOnly(value: string): string {
  return value.replace(/[^\d]/g, '');
}

function symbolCode(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9-]/g, '').slice(0, 32);
}

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

export function OrderHistoryAdmin() {
  const [userIdInput, setUserIdInput] = useState('');
  const [symbolInput, setSymbolInput] = useState('');
  const [statusInput, setStatusInput] = useState<'' | 'FILLED' | 'CANCELED'>('');
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [applied, setApplied] = useState<AdminOrderHistoryQuery>({ limit: DEFAULT_LIMIT });

  const [orders, setOrders] = useState<OrderHistory[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<Date | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const list = await fetchAdminOrderHistory(applied, controller.signal);
        if (!controller.signal.aborted) {
          setOrders(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setOrders([]);
          setError(describeError(loadError));
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => controller.abort();
  }, [applied, refreshNonce]);

  const handleSearch = (event: FormEvent) => {
    event.preventDefault();
    const trimmed = userIdInput.trim();
    let userId: number | null = null;
    if (trimmed !== '') {
      const value = Number(trimmed);
      if (!Number.isSafeInteger(value) || value <= 0) {
        setError('用户 ID 必须是正整数');
        return;
      }
      userId = value;
    }

    setApplied({
      userId,
      symbol: symbolInput || undefined,
      status: statusInput || undefined,
      limit: limitInput,
    });
  };

  const handleReset = () => {
    setUserIdInput('');
    setSymbolInput('');
    setStatusInput('');
    setLimitInput(DEFAULT_LIMIT);
    setApplied({ limit: DEFAULT_LIMIT });
  };

  return (
    <section className="panel admin-panel">
      <div className="panel-header admin-panel-header">
        <div>
          <h2>委托历史</h2>
          <span className="panel-sub">
            QuestDB ord_history · 已撤单 / 已撮合完成 · 按终态时间倒序
          </span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新委托历史"
            aria-label="刷新委托历史"
            disabled={loading}
            onClick={() => setRefreshNonce((value) => value + 1)}
          >
            <RefreshIcon spinning={loading} />
          </button>
        </div>
      </div>

      <form className="admin-toolbar" onSubmit={handleSearch}>
        <label className="admin-field narrow">
          <span>用户 ID</span>
          <input
            value={userIdInput}
            onChange={(event) => setUserIdInput(digitsOnly(event.target.value))}
            inputMode="numeric"
            placeholder="全部"
            autoComplete="off"
          />
        </label>

        <label className="admin-field narrow">
          <span>交易对</span>
          <input
            value={symbolInput}
            onChange={(event) => setSymbolInput(symbolCode(event.target.value))}
            placeholder="全部"
            autoComplete="off"
          />
        </label>

        <label className="admin-field wide">
          <span>终态</span>
          <select
            value={statusInput}
            onChange={(event) =>
              setStatusInput(event.target.value as '' | 'FILLED' | 'CANCELED')
            }
          >
            <option value="">全部</option>
            {ORDER_HISTORY_STATUSES.map((item) => (
              <option key={item.value} value={item.value}>
                {item.label}
              </option>
            ))}
          </select>
        </label>

        <label className="admin-field narrow">
          <span>条数</span>
          <select
            value={limitInput}
            onChange={(event) => setLimitInput(Number(event.target.value))}
          >
            {LIMIT_OPTIONS.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>

        <div className="admin-toolbar-actions">
          <button type="button" className="admin-button" onClick={handleReset}>
            重置
          </button>
          <button type="submit" className="admin-button primary">
            查询
          </button>
        </div>
      </form>

      {error && <div className="admin-error">{error}</div>}

      <div className="admin-summary">
        <span>
          共 <b>{orders.length}</b> 条
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">
          只归档撤单与撮合完成的委托，委托模块异步写入
        </span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table flow-table">
          <thead>
            <tr>
              <th>终态时间</th>
              <th className="align-right">委托号</th>
              <th>用户</th>
              <th>交易对</th>
              <th>方向</th>
              <th>类型</th>
              <th className="align-right">价格</th>
              <th className="align-right">数量</th>
              <th className="align-right">已成交</th>
              <th>状态</th>
              <th className="align-right">seq</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((order) => (
              <tr
                key={order.orderId}
                title={`orderId: ${order.orderId}\nrequestId: ${order.requestId}\n创建 ${formatDateTime(
                  order.createdAt,
                )} · 终态 ${formatDateTime(order.finalAt)}`}
              >
                <td className="mono muted">{formatDateTime(order.finalAt)}</td>
                <td className="mono align-right">{order.orderId}</td>
                <td className="mono">{order.userId}</td>
                <td>{order.symbol}</td>
                <td>
                  <span className={`side-pill ${order.side === 'BUY' ? 'up' : 'down'}`}>
                    {order.side === 'BUY' ? '买入' : '卖出'}
                  </span>
                </td>
                <td>
                  <span className="admin-type">
                    {order.type === 'LIMIT' ? '限价' : '市价'}
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
                  <span className={`order-status ${STATUS_CLASS[order.status] ?? 'flat'}`}>
                    {STATUS_LABEL[order.status] ?? order.status}
                  </span>
                </td>
                <td className="mono align-right muted">{order.seq}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {!loading && orders.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 QuestDB 与后端服务已启动后重试。'
              : '暂无委托历史。撤单或撮合完成的委托会由 admin 模块异步归档到这里。'}
          </div>
        )}
      </div>
    </section>
  );
}
