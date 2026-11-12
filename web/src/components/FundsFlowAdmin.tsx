import { useEffect, useState, type FormEvent } from 'react';
import {
  clearFundsFlows,
  fetchFundsFlows,
  FUNDS_LOG_TYPES,
  isAbortError,
  type Currency,
  type FundsFlow,
  type FundsFlowQuery,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime, formatUnits } from '../format';
import { RefreshIcon } from './icons';
import type { ToastKind } from './ToastStack';

interface FundsFlowAdminProps {
  currencies: Currency[];
  currencyScales: Record<string, number>;
  notify: (kind: ToastKind, text: string) => void;
}

const DEFAULT_LIMIT = 100;
const LIMIT_OPTIONS = [50, 100, 200, 500, 1000];

const OP_LABELS: Record<string, string> = {
  AVAILABLE_CREDIT: '可用入账',
  AVAILABLE_DEBIT: '可用扣账',
  FROZEN_CREDIT: '冻结入账',
  FROZEN_DEBIT: '冻结扣账',
  AVAILABLE_TO_FROZEN: '可用转冻结',
  FROZEN_TO_AVAILABLE: '冻结转可用',
};

const OP_CLASS: Record<string, string> = {
  AVAILABLE_CREDIT: 'up',
  FROZEN_CREDIT: 'up',
  AVAILABLE_DEBIT: 'down',
  FROZEN_DEBIT: 'down',
  AVAILABLE_TO_FROZEN: 'flat',
  FROZEN_TO_AVAILABLE: 'flat',
};

function digitsOnly(value: string): string {
  return value.replace(/[^\d]/g, '');
}

function currencyCode(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 16);
}

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

export function FundsFlowAdmin({ currencies, currencyScales, notify }: FundsFlowAdminProps) {
  const [userIdInput, setUserIdInput] = useState('');
  const [currencyInput, setCurrencyInput] = useState('');
  const [logTypeInput, setLogTypeInput] = useState('');
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [applied, setApplied] = useState<FundsFlowQuery>({ limit: DEFAULT_LIMIT });

  const [flows, setFlows] = useState<FundsFlow[]>([]);
  const [loading, setLoading] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<Date | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const list = await fetchFundsFlows(applied, controller.signal);
        if (!controller.signal.aborted) {
          setFlows(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setFlows([]);
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
      currency: currencyInput || undefined,
      logType: logTypeInput || undefined,
      limit: limitInput,
    });
  };

  const handleReset = () => {
    setUserIdInput('');
    setCurrencyInput('');
    setLogTypeInput('');
    setLimitInput(DEFAULT_LIMIT);
    setApplied({ limit: DEFAULT_LIMIT });
  };

  const handleClear = async () => {
    if (clearing) {
      return;
    }
    const confirmed = window.confirm(
      '确定要清空 QuestDB 中全部资金流水吗？\n\n'
        + '将删除所有用户、所有币种的流水（不只是当前筛选结果），操作不可恢复。',
    );
    if (!confirmed) {
      return;
    }

    setClearing(true);
    try {
      const deletedCount = await clearFundsFlows();
      notify('success', `已清空 ${deletedCount.toLocaleString('zh-CN')} 条资金流水`);
      setFlows([]);
      setLoadedAt(new Date());
      setRefreshNonce((value) => value + 1);
    } catch (clearError) {
      notify('error', describeError(clearError));
    } finally {
      setClearing(false);
    }
  };

  return (
    <section className="panel admin-panel">
      <div className="panel-header admin-panel-header">
        <div>
          <h2>资金流水</h2>
          <span className="panel-sub">QuestDB funds_flow · 按发生时间倒序</span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新流水"
            aria-label="刷新流水"
            disabled={loading}
            onClick={() => setRefreshNonce((value) => value + 1)}
          >
            <RefreshIcon spinning={loading} />
          </button>
          <button
            type="button"
            className="admin-button danger"
            disabled={clearing || loading}
            onClick={handleClear}
          >
            {clearing ? '清空中…' : '一键删除'}
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
          <span>币种</span>
          <input
            value={currencyInput}
            onChange={(event) => setCurrencyInput(currencyCode(event.target.value))}
            list="flow-currency-options"
            placeholder="全部"
            autoComplete="off"
          />
          <datalist id="flow-currency-options">
            {currencies.map((currency) => (
              <option key={currency.code} value={currency.code} />
            ))}
          </datalist>
        </label>

        <label className="admin-field wide">
          <span>日志类型</span>
          <select
            value={logTypeInput}
            onChange={(event) => setLogTypeInput(event.target.value)}
          >
            <option value="">全部</option>
            {FUNDS_LOG_TYPES.map((item) => (
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
          共 <b>{flows.length}</b> 条
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">删除为整表清空，只影响 QuestDB 查询投影</span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table flow-table">
          <thead>
            <tr>
              <th>时间</th>
              <th>用户</th>
              <th>币种</th>
              <th>日志类型</th>
              <th>原子操作</th>
              <th className="align-right">金额</th>
              <th className="align-right">可用余额（前 → 后）</th>
              <th className="align-right">冻结余额（前 → 后）</th>
              <th className="align-right">seq</th>
            </tr>
          </thead>
          <tbody>
            {flows.map((flow) => {
              const scale = currencyScales[flow.currency] ?? 0;
              return (
                <tr
                  key={`${flow.eventId}-${flow.seq}-${flow.occurredAt}`}
                  title={`eventId: ${flow.eventId}\nrequestId: ${flow.requestId}\n${flow.logDesc}`}
                >
                  <td className="mono muted">{formatDateTime(flow.occurredAt)}</td>
                  <td className="mono">{flow.userId}</td>
                  <td>{flow.currency}</td>
                  <td>
                    <span className="admin-type">{flow.logType}</span>
                  </td>
                  <td>
                    <span className={`side-pill ${OP_CLASS[flow.op] ?? 'flat'}`}>
                      {OP_LABELS[flow.op] ?? flow.op}
                    </span>
                  </td>
                  <td className="mono align-right">{formatUnits(flow.amount, scale)}</td>
                  <td className="mono align-right">
                    <span className="admin-balance">
                      {formatUnits(flow.availableBefore, scale)}
                      <i>→</i>
                      <b>{formatUnits(flow.availableAfter, scale)}</b>
                    </span>
                  </td>
                  <td className="mono align-right">
                    <span className="admin-balance">
                      {formatUnits(flow.frozenBefore, scale)}
                      <i>→</i>
                      <b>{formatUnits(flow.frozenAfter, scale)}</b>
                    </span>
                  </td>
                  <td className="mono align-right muted">{flow.seq}</td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {!loading && flows.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 QuestDB 与后端服务已启动后重试。'
              : '暂无流水记录。可以调整筛选条件，或先通过「模拟登录」充值产生流水。'}
          </div>
        )}
      </div>
    </section>
  );
}
