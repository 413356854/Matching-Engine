import { useEffect, useMemo, useState, type FormEvent } from 'react';
import {
  fetchAdminUserTrades,
  isAbortError,
  USER_TRADE_ROLES,
  type AdminUserTradeQuery,
  type TradingPair,
  type UserTrade,
  type UserTradeRole,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime, formatPrice, formatQuantity, formatUnits } from '../format';
import { RefreshIcon } from './icons';

interface UserTradeAdminProps {
  pairs: TradingPair[];
  currencyScales: Record<string, number>;
}

const DEFAULT_LIMIT = 100;
const LIMIT_OPTIONS = [50, 100, 200, 500, 1000];

function digitsOnly(value: string): string {
  return value.replace(/[^\d]/g, '');
}

function symbolCode(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9-]/g, '').slice(0, 32);
}

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

export function UserTradeAdmin({ pairs, currencyScales }: UserTradeAdminProps) {
  const [userIdInput, setUserIdInput] = useState('');
  const [symbolInput, setSymbolInput] = useState('');
  const [roleInput, setRoleInput] = useState<'' | UserTradeRole>('');
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [applied, setApplied] = useState<AdminUserTradeQuery>({ limit: DEFAULT_LIMIT });

  const [trades, setTrades] = useState<UserTrade[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<Date | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);

  const pairMap = useMemo(
    () => new Map(pairs.map((pair) => [pair.symbol, pair])),
    [pairs],
  );

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const list = await fetchAdminUserTrades(applied, controller.signal);
        if (!controller.signal.aborted) {
          setTrades(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setTrades([]);
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
      role: roleInput || undefined,
      limit: limitInput,
    });
  };

  const handleReset = () => {
    setUserIdInput('');
    setSymbolInput('');
    setRoleInput('');
    setLimitInput(DEFAULT_LIMIT);
    setApplied({ limit: DEFAULT_LIMIT });
  };

  return (
    <section className="panel admin-panel">
      <div className="panel-header admin-panel-header">
        <div>
          <h2>成交记录</h2>
          <span className="panel-sub">
            QuestDB user_trade · 每个撮合对拆成 taker / maker 两条 · 按成交时间倒序
          </span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新成交记录"
            aria-label="刷新成交记录"
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
            list="user-trade-symbol-options"
            placeholder="全部"
            autoComplete="off"
          />
          <datalist id="user-trade-symbol-options">
            {pairs.map((pair) => (
              <option key={pair.symbol} value={pair.symbol} />
            ))}
          </datalist>
        </label>

        <label className="admin-field wide">
          <span>角色</span>
          <select
            value={roleInput}
            onChange={(event) =>
              setRoleInput(event.target.value as '' | UserTradeRole)
            }
          >
            <option value="">全部</option>
            {USER_TRADE_ROLES.map((item) => (
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
          共 <b>{trades.length}</b> 条
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">
          TAKER 为主动方，MAKER 为被动方，双方共享同一成交号
        </span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table flow-table">
          <thead>
            <tr>
              <th>交易对</th>
              <th>成交时间</th>
              <th>用户</th>
              <th className="align-right">价格</th>
              <th className="align-right">数量</th>
              <th className="align-right">成交额</th>
              <th>角色</th>
              <th>方向</th>
              <th className="align-right">成交号</th>
              <th className="align-right">委托号</th>
              <th className="align-right">对手单号</th>
            </tr>
          </thead>
          <tbody>
            {trades.map((trade) => {
              const pair = pairMap.get(trade.symbol);
              const priceScale = pair?.priceScale ?? 0;
              const quantityScale = pair?.quantityScale ?? 0;
              const quoteCurrency =
                pair?.quoteCurrency ?? trade.symbol.split('-')[1] ?? '';
              const amountScale = currencyScales[quoteCurrency] ?? 0;
              return (
                <tr
                  key={`${trade.tradeId}-${trade.orderId}`}
                  title={`tradeId: ${trade.tradeId}\norderId: ${trade.orderId}\ncounterOrderId: ${trade.counterOrderId}`}
                >
                  <td>{trade.symbol}</td>
                  <td className="mono muted">{formatDateTime(trade.executedAt)}</td>
                  <td className="mono">{trade.userId}</td>
                  <td className="mono align-right">
                    {formatPrice(trade.price, priceScale)}
                  </td>
                  <td className="mono align-right">
                    {formatQuantity(trade.quantity, quantityScale)}
                  </td>
                  <td className="mono align-right">
                    {formatUnits(trade.amount, amountScale)}
                  </td>
                  <td>
                    <span className="admin-type">{trade.role}</span>
                  </td>
                  <td>
                    {trade.side ? (
                      <span className={`side-pill ${trade.side === 'BUY' ? 'up' : 'down'}`}>
                        {trade.side === 'BUY' ? '买入' : '卖出'}
                      </span>
                    ) : (
                      <span className="muted">--</span>
                    )}
                  </td>
                  <td className="mono align-right">{trade.tradeId}</td>
                  <td className="mono align-right">{trade.orderId}</td>
                  <td className="mono align-right muted">{trade.counterOrderId}</td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {!loading && trades.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 QuestDB 与后端服务已启动后重试。'
              : '暂无用户成交记录。撮合产生成交后 admin 模块会异步归档到这里。'}
          </div>
        )}
      </div>
    </section>
  );
}
