import { useEffect, useMemo, useState, type FormEvent, type MouseEvent } from 'react';
import {
  createAdminPair,
  deleteAdminPair,
  fetchAdminPairs,
  isAbortError,
  updateAdminPair,
  type AdminPairUpdateInput,
  type Currency,
  type TradingPair,
} from '../api';
import { describeError } from '../errors';
import {
  formatDisplayUnits,
  formatInputValue,
  formatUnits,
  parseUnits,
  quantityStep,
  toSafeNumber,
} from '../format';
import { RefreshIcon } from './icons';
import type { ToastKind } from './ToastStack';

interface PairAdminProps {
  currencies: Currency[];
  notify: (kind: ToastKind, text: string) => void;
  /** 交易对增删改后通知外层刷新 `/api/v1/pairs`，让交易页立即看到新配置。 */
  onPairsChanged: () => void;
}

interface PairFormState {
  mode: 'create' | 'edit';
  pair: TradingPair | null;
}

const STATUS_ONLINE = 1;
const STATUS_OFFLINE = 0;
const MAX_SCALE = 18;
const MAX_FEE_BPS = 10_000;
const LONG_MAX = 9_223_372_036_854_775_807n;

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

function digitsOnly(value: string): string {
  return value.replace(/[^\d]/g, '');
}

/** 允许前导负号，供费率输入使用。 */
function signedDigits(value: string): string {
  const negative = value.trimStart().startsWith('-');
  const digits = digitsOnly(value);
  return negative ? `-${digits}` : digits;
}

/** 只保留数字和一个小数点。 */
function decimalText(value: string): string {
  const cleaned = value.replace(/[^\d.]/g, '');
  const dot = cleaned.indexOf('.');
  if (dot === -1) {
    return cleaned;
  }
  return cleaned.slice(0, dot + 1) + cleaned.slice(dot + 1).replace(/\./g, '');
}

/** 展示单位输入 → 最小单位 number；非法或超出安全整数返回 null。 */
function parseLedgerAmount(value: string, scale: number): number | null {
  const parsed = parseUnits(value, scale);
  if (parsed === null || parsed > LONG_MAX) {
    return null;
  }
  return toSafeNumber(parsed);
}

function toUpdateInput(pair: TradingPair, status: number): AdminPairUpdateInput {
  return {
    priceScale: pair.priceScale,
    quantityScale: pair.quantityScale,
    minOrderQuantity: pair.minOrderQuantity,
    minOrderAmount: pair.minOrderAmount,
    makerFeeBps: pair.makerFeeBps,
    takerFeeBps: pair.takerFeeBps,
    status,
    sortOrder: pair.sortOrder,
  };
}

export function PairAdmin({ currencies, notify, onPairsChanged }: PairAdminProps) {
  const [pairs, setPairs] = useState<TradingPair[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<Date | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [form, setForm] = useState<PairFormState | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);

  const currencyMap = useMemo(
    () => new Map(currencies.map((currency) => [currency.code, currency])),
    [currencies],
  );

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const list = await fetchAdminPairs(controller.signal);
        if (!controller.signal.aborted) {
          setPairs(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setPairs([]);
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
  }, [refreshNonce]);

  const refresh = () => setRefreshNonce((value) => value + 1);

  const handleSaved = (saved: TradingPair) => {
    const created = form?.mode === 'create';
    setForm(null);
    notify('success', `交易对 ${saved.symbol} 已${created ? '创建' : '保存'}`);
    refresh();
    onPairsChanged();
  };

  const handleToggleStatus = async (pair: TradingPair) => {
    if (busyId !== null) {
      return;
    }
    const nextStatus = pair.status === STATUS_ONLINE ? STATUS_OFFLINE : STATUS_ONLINE;
    setBusyId(pair.id);
    try {
      const saved = await updateAdminPair(pair.id, toUpdateInput(pair, nextStatus));
      notify(
        'success',
        `交易对 ${saved.symbol} 已${nextStatus === STATUS_ONLINE ? '上线' : '下线'}`,
      );
      refresh();
      onPairsChanged();
    } catch (toggleError) {
      notify('error', describeError(toggleError));
    } finally {
      setBusyId(null);
    }
  };

  const handleDelete = async (pair: TradingPair) => {
    if (busyId !== null) {
      return;
    }
    if (pair.status !== STATUS_OFFLINE) {
      notify('error', '交易对仍在线，请先下线再删除');
      return;
    }
    const confirmed = window.confirm(
      `确定删除交易对 ${pair.symbol} 吗？\n\n`
        + '删除后下单校验会立即拒绝该交易对，无需重启；'
        + '已有的委托历史与成交记录会保留。'
        + '订单簿仍有挂单时后端会拒绝删除，请先撤销全部委托。此操作不可恢复。',
    );
    if (!confirmed) {
      return;
    }

    setBusyId(pair.id);
    try {
      await deleteAdminPair(pair.id);
      notify('success', `交易对 ${pair.symbol} 已删除`);
      refresh();
      onPairsChanged();
    } catch (deleteError) {
      notify('error', describeError(deleteError));
    } finally {
      setBusyId(null);
    }
  };

  const onlineCount = pairs.filter((pair) => pair.status === STATUS_ONLINE).length;
  const nextSortOrder = pairs.reduce((max, pair) => Math.max(max, pair.sortOrder), 0) + 1;

  return (
    <section className="panel admin-panel">
      <div className="panel-header admin-panel-header">
        <div>
          <h2>交易对</h2>
          <span className="panel-sub">
            MySQL pair_symbol · 修改后立即刷新配置缓存，无需重启
          </span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新交易对"
            aria-label="刷新交易对"
            disabled={loading}
            onClick={refresh}
          >
            <RefreshIcon spinning={loading} />
          </button>
          <button
            type="button"
            className="admin-button primary"
            onClick={() => setForm({ mode: 'create', pair: null })}
          >
            新增交易对
          </button>
        </div>
      </div>

      {error && <div className="admin-error">{error}</div>}

      <div className="admin-summary">
        <span>
          共 <b>{pairs.length}</b> 个 · 在线 <b>{onlineCount}</b> · 下线{' '}
          <b>{pairs.length - onlineCount}</b>
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">
          数量精度跟随基础币种；删除需先下线；修改对下单校验立即生效
        </span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table pair-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>交易对</th>
              <th>基础 / 计价</th>
              <th className="align-right">价格精度</th>
              <th className="align-right">数量精度</th>
              <th className="align-right">最小下单量</th>
              <th className="align-right">最小下单金额</th>
              <th className="align-right">maker / taker</th>
              <th>状态</th>
              <th className="align-right">排序</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {pairs.map((pair) => {
              const quoteScale = currencyMap.get(pair.quoteCurrency)?.rawScale ?? 0;
              const busy = busyId === pair.id;
              return (
                <tr key={pair.id}>
                  <td className="mono muted">#{pair.id}</td>
                  <td className="mono pair-symbol">{pair.symbol}</td>
                  <td>
                    <span className="admin-type">
                      {pair.baseCurrency} / {pair.quoteCurrency}
                    </span>
                  </td>
                  <td className="mono align-right">{pair.priceScale}</td>
                  <td className="mono align-right">{pair.quantityScale}</td>
                  <td
                    className="mono align-right"
                    title={`${pair.minOrderQuantity.toLocaleString('zh-CN')} ${pair.baseCurrency} 最小单位`}
                  >
                    {pair.minOrderQuantity > 0
                      ? `${formatUnits(pair.minOrderQuantity, pair.quantityScale)} ${pair.baseCurrency}`
                      : '不限'}
                  </td>
                  <td
                    className="mono align-right"
                    title={`${pair.minOrderAmount.toLocaleString('zh-CN')} ${pair.quoteCurrency} 最小单位`}
                  >
                    {pair.minOrderAmount > 0
                      ? `${formatDisplayUnits(
                        pair.minOrderAmount,
                        quoteScale,
                        pair.priceScale,
                      )} ${pair.quoteCurrency}`
                      : '不限'}
                  </td>
                  <td className="mono align-right">
                    {pair.makerFeeBps} / {pair.takerFeeBps} bps
                  </td>
                  <td>
                    <span
                      className={`side-pill ${
                        pair.status === STATUS_ONLINE ? 'up' : 'down'
                      }`}
                    >
                      {pair.status === STATUS_ONLINE ? '在线' : '下线'}
                    </span>
                  </td>
                  <td className="mono align-right muted">{pair.sortOrder}</td>
                  <td>
                    <div className="pair-actions">
                      <button
                        type="button"
                        className="admin-button"
                        disabled={busy}
                        onClick={() => setForm({ mode: 'edit', pair })}
                      >
                        编辑
                      </button>
                      <button
                        type="button"
                        className="admin-button"
                        disabled={busy}
                        onClick={() => void handleToggleStatus(pair)}
                      >
                        {busy
                          ? '处理中…'
                          : pair.status === STATUS_ONLINE
                            ? '下线'
                            : '上线'}
                      </button>
                      <button
                        type="button"
                        className="admin-button danger"
                        disabled={busy || pair.status !== STATUS_OFFLINE}
                        title={
                          pair.status !== STATUS_OFFLINE
                            ? '请先下线再删除'
                            : `删除 ${pair.symbol}`
                        }
                        onClick={() => void handleDelete(pair)}
                      >
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {!loading && pairs.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 MySQL 表 pair_symbol 已创建、后端已启动后重试。'
              : '暂无交易对。点击「新增交易对」创建第一个市场。'}
          </div>
        )}
      </div>

      {form && (
        <PairFormModal
          key={form.mode === 'create' ? 'create' : `edit-${form.pair?.id ?? 0}`}
          mode={form.mode}
          pair={form.pair}
          currencies={currencies}
          defaultSortOrder={nextSortOrder}
          onClose={() => setForm(null)}
          onSaved={handleSaved}
        />
      )}
    </section>
  );
}

interface PairFormModalProps {
  mode: 'create' | 'edit';
  pair: TradingPair | null;
  currencies: Currency[];
  /** 新建时默认排到现有交易对之后。 */
  defaultSortOrder: number;
  onClose: () => void;
  onSaved: (pair: TradingPair) => void;
}

function PairFormModal({
  mode,
  pair,
  currencies,
  defaultSortOrder,
  onClose,
  onSaved,
}: PairFormModalProps) {
  const options = useMemo(
    () => [...currencies].sort((left, right) => left.sortOrder - right.sortOrder),
    [currencies],
  );
  const currencyMap = useMemo(
    () => new Map(options.map((currency) => [currency.code, currency])),
    [options],
  );

  const defaultBase = pair?.baseCurrency ?? options[0]?.code ?? '';
  const defaultQuote =
    pair?.quoteCurrency
    ?? options.find((currency) => currency.code === 'USDT' && currency.code !== defaultBase)?.code
    ?? options.find((currency) => currency.code !== defaultBase)?.code
    ?? '';

  const [baseInput, setBaseInput] = useState(defaultBase);
  const [quoteInput, setQuoteInput] = useState(defaultQuote);
  const [priceScaleInput, setPriceScaleInput] = useState(String(pair?.priceScale ?? 2));
  const [minQuantityInput, setMinQuantityInput] = useState(() =>
    pair ? formatInputValue(pair.minOrderQuantity, pair.quantityScale) : '0',
  );
  const [minAmountInput, setMinAmountInput] = useState(() => {
    if (!pair) {
      return '0';
    }
    const quoteScale = currencyMap.get(pair.quoteCurrency)?.rawScale ?? 0;
    return formatInputValue(pair.minOrderAmount, quoteScale);
  });
  const [makerFeeInput, setMakerFeeInput] = useState(String(pair?.makerFeeBps ?? 10));
  const [takerFeeInput, setTakerFeeInput] = useState(String(pair?.takerFeeBps ?? 20));
  const [statusInput, setStatusInput] = useState(String(pair?.status ?? STATUS_ONLINE));
  const [sortOrderInput, setSortOrderInput] = useState(
    String(pair?.sortOrder ?? defaultSortOrder),
  );
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !submitting) {
        onClose();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose, submitting]);

  const baseCurrency = currencyMap.get(baseInput);
  const quoteCurrency = currencyMap.get(quoteInput);
  const quantityScale = baseCurrency?.rawScale ?? pair?.quantityScale ?? 0;
  const priceScaleNumber = Number(priceScaleInput);
  const derivedQuantityStep =
    baseCurrency && quoteCurrency
    && Number.isInteger(priceScaleNumber) && priceScaleNumber >= 0
      ? quantityStep(priceScaleNumber, baseCurrency.rawScale, quoteCurrency.rawScale)
      : null;
  const symbolPreview =
    baseInput && quoteInput ? `${baseInput}-${quoteInput}` : 'BASE-QUOTE';

  const parsedMinQuantity = parseLedgerAmount(minQuantityInput, quantityScale);
  const parsedMinAmount = quoteCurrency
    ? parseLedgerAmount(minAmountInput, quoteCurrency.rawScale)
    : null;

  const handleBackdrop = (event: MouseEvent<HTMLDivElement>) => {
    if (event.target === event.currentTarget && !submitting) {
      onClose();
    }
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (submitting) {
      return;
    }
    if (!baseInput) {
      setError('请选择基础币种');
      return;
    }
    if (!quoteInput) {
      setError('请选择计价币种');
      return;
    }
    if (baseInput === quoteInput) {
      setError('基础币种和计价币种不能相同');
      return;
    }
    if (!baseCurrency) {
      setError('基础币种不存在或未启用');
      return;
    }
    if (!quoteCurrency) {
      setError('计价币种不存在或未启用');
      return;
    }

    const priceScale = Number(priceScaleInput);
    if (!Number.isInteger(priceScale) || priceScale < 0 || priceScale > MAX_SCALE) {
      setError(`价格精度必须是 0~${MAX_SCALE} 的整数`);
      return;
    }
    if (mode === 'create' && priceScale > quoteCurrency.rawScale) {
      setError(
        `价格精度不能高于计价币 ${quoteInput} 的 raw_scale ${quoteCurrency.rawScale}；`
        + '否则 amount 无法换算回计价币原始整数',
      );
      return;
    }
    const minOrderQuantity = parseLedgerAmount(minQuantityInput, baseCurrency.rawScale);
    if (minOrderQuantity === null) {
      setError(`最小下单量最多 ${baseCurrency.rawScale} 位小数，或超出可表示范围`);
      return;
    }
    const minOrderAmount = parseLedgerAmount(minAmountInput, quoteCurrency.rawScale);
    if (minOrderAmount === null) {
      setError(`最小下单金额最多 ${quoteCurrency.rawScale} 位小数，或超出可表示范围`);
      return;
    }
    const makerFeeBps = Number(makerFeeInput);
    if (
      !Number.isInteger(makerFeeBps)
      || makerFeeBps < -MAX_FEE_BPS
      || makerFeeBps > MAX_FEE_BPS
    ) {
      setError(`maker 费率必须是 -${MAX_FEE_BPS}~${MAX_FEE_BPS} 的整数 bps`);
      return;
    }
    const takerFeeBps = Number(takerFeeInput);
    if (
      !Number.isInteger(takerFeeBps)
      || takerFeeBps < -MAX_FEE_BPS
      || takerFeeBps > MAX_FEE_BPS
    ) {
      setError(`taker 费率必须是 -${MAX_FEE_BPS}~${MAX_FEE_BPS} 的整数 bps`);
      return;
    }
    const sortOrder = Number(sortOrderInput);
    if (!Number.isInteger(sortOrder) || sortOrder < 0) {
      setError('排序必须是非负整数');
      return;
    }

    const input = {
      priceScale,
      quantityScale: baseCurrency.rawScale,
      minOrderQuantity,
      minOrderAmount,
      makerFeeBps,
      takerFeeBps,
      status: Number(statusInput) === STATUS_ONLINE ? STATUS_ONLINE : STATUS_OFFLINE,
      sortOrder,
    };

    setSubmitting(true);
    setError(null);
    try {
      const saved =
        mode === 'create'
          ? await createAdminPair({
              baseCurrency: baseInput,
              quoteCurrency: quoteInput,
              ...input,
            })
          : await updateAdminPair(
              pair?.id ?? 0,
              input,
            );
      onSaved(saved);
    } catch (submitError) {
      setError(describeError(submitError));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="modal-backdrop" onMouseDown={handleBackdrop}>
      <div
        className="modal-card pair-form-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby="pair-form-title"
      >
        <div className="modal-head">
          <div>
            <h2 id="pair-form-title">
              {mode === 'create' ? '新增交易对' : `编辑 ${pair?.symbol ?? ''}`}
            </h2>
            <p className="modal-sub">
              {mode === 'create'
                ? '交易对代码由基础币种和计价币种自动派生，数量精度跟随基础币种。'
                : '交易对代码、币种与价格 / 数量精度创建后不可修改；可调整限额、费率、状态与排序。'}
            </p>
          </div>
          <button
            type="button"
            className="modal-close"
            onClick={onClose}
            disabled={submitting}
            aria-label="关闭"
          >
            ×
          </button>
        </div>

        <form className="modal-body" onSubmit={handleSubmit}>
          <div className="pair-form-grid">
            <label className="modal-field">
              <span>基础币种</span>
              {mode === 'create' ? (
                <select
                  value={baseInput}
                  onChange={(event) => setBaseInput(event.target.value)}
                  disabled={submitting}
                >
                  {options.map((currency) => (
                    <option key={currency.code} value={currency.code}>
                      {currency.code} · {currency.name}（raw {currency.rawScale} / 展示 {currency.scale}）
                    </option>
                  ))}
                </select>
              ) : (
                <input value={baseInput} disabled />
              )}
            </label>

            <label className="modal-field">
              <span>计价币种</span>
              {mode === 'create' ? (
                <select
                  value={quoteInput}
                  onChange={(event) => setQuoteInput(event.target.value)}
                  disabled={submitting}
                >
                  {options.map((currency) => (
                    <option
                      key={currency.code}
                      value={currency.code}
                      disabled={currency.code === baseInput}
                    >
                      {currency.code} · {currency.name}（raw {currency.rawScale} / 展示 {currency.scale}）
                    </option>
                  ))}
                </select>
              ) : (
                <input value={quoteInput} disabled />
              )}
            </label>

            <label className="modal-field">
              <span>
                交易对代码 <em>自动派生</em>
              </span>
              <input value={symbolPreview} disabled />
            </label>

            <label className="modal-field">
              <span>
                数量精度 <em>跟随基础币种 raw_scale</em>
              </span>
              <input value={baseCurrency ? String(baseCurrency.rawScale) : '--'} disabled />
            </label>

            <label className="modal-field">
              <span>
                价格精度{' '}
                <em>{mode === 'edit' ? '创建后不可改' : `0~${MAX_SCALE}`}</em>
              </span>
              <input
                value={priceScaleInput}
                onChange={(event) => setPriceScaleInput(digitsOnly(event.target.value))}
                inputMode="numeric"
                maxLength={2}
                autoComplete="off"
                disabled={mode === 'edit' || submitting}
              />
            </label>

            <label className="modal-field">
              <span>
                最小下单量（{baseInput || '基础币'}） <em>0 = 不限</em>
              </span>
              <input
                value={minQuantityInput}
                onChange={(event) => setMinQuantityInput(decimalText(event.target.value))}
                inputMode="decimal"
                autoComplete="off"
                disabled={submitting}
              />
            </label>

            <label className="modal-field">
              <span>
                最小下单金额（{quoteInput || '计价币'}） <em>0 = 不限</em>
              </span>
              <input
                value={minAmountInput}
                onChange={(event) => setMinAmountInput(decimalText(event.target.value))}
                inputMode="decimal"
                autoComplete="off"
                disabled={submitting}
              />
            </label>

            <label className="modal-field">
              <span>
                maker 费率 <em>bps，负值返佣</em>
              </span>
              <input
                value={makerFeeInput}
                onChange={(event) => setMakerFeeInput(signedDigits(event.target.value))}
                inputMode="numeric"
                autoComplete="off"
                disabled={submitting}
              />
            </label>

            <label className="modal-field">
              <span>
                taker 费率 <em>bps，负值返佣</em>
              </span>
              <input
                value={takerFeeInput}
                onChange={(event) => setTakerFeeInput(signedDigits(event.target.value))}
                inputMode="numeric"
                autoComplete="off"
                disabled={submitting}
              />
            </label>

            <label className="modal-field">
              <span>上线状态</span>
              <select
                value={statusInput}
                onChange={(event) => setStatusInput(event.target.value)}
                disabled={submitting}
              >
                <option value={STATUS_ONLINE}>上线（允许下单）</option>
                <option value={STATUS_OFFLINE}>下线（拒绝新委托）</option>
              </select>
            </label>

            <label className="modal-field">
              <span>
                排序 <em>升序</em>
              </span>
              <input
                value={sortOrderInput}
                onChange={(event) => setSortOrderInput(digitsOnly(event.target.value))}
                inputMode="numeric"
                maxLength={6}
                autoComplete="off"
                disabled={submitting}
              />
            </label>
          </div>

          <div className="pair-form-previews">
            <div className="pair-form-preview">
              <span>最小下单量</span>
              <b>
                {parsedMinQuantity === null
                  ? '--'
                  : `${parsedMinQuantity.toLocaleString('zh-CN')} 最小单位`}
              </b>
            </div>
            <div className="pair-form-preview">
              <span>最小下单金额</span>
              <b>
                {parsedMinAmount === null
                  ? '--'
                  : `${parsedMinAmount.toLocaleString('zh-CN')} 最小单位`}
              </b>
            </div>
          </div>

          <p className="modal-hint">
            最小下单量按{baseInput || '基础币'} raw_scale
            {baseCurrency ? ` ${baseCurrency.rawScale}` : ''} 换算，最小下单金额按
            {quoteInput || '计价币'} raw_scale
            {quoteCurrency ? ` ${quoteCurrency.rawScale}` : ''} 换算；两者为 0 表示不限制。
            价格精度不能高于计价币 raw_scale，否则 amount 无法换算回原始整数。
            {derivedQuantityStep !== null && baseCurrency && (
              <>
                {' '}当前配置的最小数量步长为{' '}
                {formatInputValue(derivedQuantityStep, baseCurrency.rawScale)}{' '}
                {baseInput || '基础币'}。
              </>
            )}
            费率单位是基点（1 bps = 0.01%）。保存后会刷新配置缓存，下单校验立即生效。
          </p>

          {error && <div className="modal-error">{error}</div>}

          <div className="modal-actions">
            <button
              type="button"
              className="modal-cancel"
              onClick={onClose}
              disabled={submitting}
            >
              取消
            </button>
            <button type="submit" className="modal-submit" disabled={submitting}>
              {submitting
                ? '保存中…'
                : mode === 'create'
                  ? '创建交易对'
                  : '保存修改'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
