import { useEffect, useMemo, useState, type FormEvent } from 'react';
import {
  clearAllKlines,
  fetchAdminKlines,
  isAbortError,
  KLINE_INTERVALS,
  type AdminKlineQuery,
  type Kline,
  type TradingPair,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime, formatPrice, formatQuantity, formatUnits } from '../format';
import { RefreshIcon } from './icons';
import type { ToastKind } from './ToastStack';

interface KlineAdminProps {
  pairs: TradingPair[];
  currencyScales: Record<string, number>;
  notify: (kind: ToastKind, text: string) => void;
}

const DEFAULT_LIMIT = 100;
const LIMIT_OPTIONS = [100, 200, 500, 1000];

function symbolText(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9-]/g, '').slice(0, 32);
}

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

export function KlineAdmin({ pairs, currencyScales, notify }: KlineAdminProps) {
  const [symbolInput, setSymbolInput] = useState('');
  const [intervalInput, setIntervalInput] = useState('');
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [applied, setApplied] = useState<AdminKlineQuery>({ limit: DEFAULT_LIMIT });

  const [klines, setKlines] = useState<Kline[]>([]);
  const [loading, setLoading] = useState(false);
  const [clearing, setClearing] = useState(false);
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
        const list = await fetchAdminKlines(applied, controller.signal);
        if (!controller.signal.aborted) {
          setKlines(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setKlines([]);
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
    setApplied({
      symbol: symbolInput || undefined,
      interval: intervalInput || undefined,
      limit: limitInput,
    });
  };

  const handleReset = () => {
    setSymbolInput('');
    setIntervalInput('');
    setLimitInput(DEFAULT_LIMIT);
    setApplied({ limit: DEFAULT_LIMIT });
  };

  const handleClear = async () => {
    if (clearing) {
      return;
    }
    const confirmed = window.confirm(
      '确定要清空 QuestDB 中全部 K 线吗？\n\n'
        + '将删除所有交易对、所有周期的 K 线（不只是当前筛选结果），成交明细 md_trade 不受影响，操作不可恢复。',
    );
    if (!confirmed) {
      return;
    }

    setClearing(true);
    try {
      const deletedCount = await clearAllKlines();
      notify('success', `已清空 ${deletedCount.toLocaleString('zh-CN')} 条 K 线`);
      setKlines([]);
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
          <h2>K 线</h2>
          <span className="panel-sub">QuestDB md_kline · 按 bucket 起点倒序</span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新 K 线"
            aria-label="刷新 K 线"
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
        <label className="admin-field wide">
          <span>交易对</span>
          <input
            value={symbolInput}
            onChange={(event) => setSymbolInput(symbolText(event.target.value))}
            list="kline-symbol-options"
            placeholder="全部"
            autoComplete="off"
          />
          <datalist id="kline-symbol-options">
            {pairs.map((pair) => (
              <option key={pair.symbol} value={pair.symbol} />
            ))}
          </datalist>
        </label>

        <label className="admin-field narrow">
          <span>周期</span>
          <select
            value={intervalInput}
            onChange={(event) => setIntervalInput(event.target.value)}
          >
            <option value="">全部</option>
            {KLINE_INTERVALS.map((interval) => (
              <option key={interval} value={interval}>
                {interval}
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
          共 <b>{klines.length}</b> 条
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">删除为整表清空，只影响 QuestDB 中的 K 线投影</span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table kline-table">
          <thead>
            <tr>
              <th>开始时间</th>
              <th>交易对</th>
              <th>周期</th>
              <th className="align-right">开盘</th>
              <th className="align-right">最高</th>
              <th className="align-right">最低</th>
              <th className="align-right">收盘</th>
              <th className="align-right">成交量</th>
              <th className="align-right">成交额</th>
              <th className="align-right">笔数</th>
            </tr>
          </thead>
          <tbody>
            {klines.map((kline) => {
              const pair = pairMap.get(kline.symbol);
              const priceScale = pair?.priceScale ?? 0;
              const quantityScale = pair?.quantityScale ?? 0;
              const quoteCurrency = pair?.quoteCurrency ?? kline.symbol.split('-')[1] ?? '';
              const amountScale = currencyScales[quoteCurrency] ?? 0;
              return (
                <tr key={`${kline.symbol}-${kline.interval}-${kline.bucketStart}`}>
                  <td className="mono muted">{formatDateTime(kline.bucketStart)}</td>
                  <td>{kline.symbol}</td>
                  <td>
                    <span className="admin-type">{kline.interval}</span>
                  </td>
                  <td className="mono align-right">{formatPrice(kline.openPrice, priceScale)}</td>
                  <td className="mono align-right">{formatPrice(kline.highPrice, priceScale)}</td>
                  <td className="mono align-right">{formatPrice(kline.lowPrice, priceScale)}</td>
                  <td className="mono align-right">{formatPrice(kline.closePrice, priceScale)}</td>
                  <td className="mono align-right">
                    {formatQuantity(kline.volume, quantityScale)}
                  </td>
                  <td className="mono align-right">{formatUnits(kline.amount, amountScale)}</td>
                  <td className="mono align-right muted">{kline.tradeCount}</td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {!loading && klines.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 QuestDB 与后端服务已启动后重试。'
              : '暂无 K 线记录。产生成交后行情模块会自动写入 QuestDB。'}
          </div>
        )}
      </div>
    </section>
  );
}
