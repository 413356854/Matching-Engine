import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import {
  ApiError,
  placeOrder,
  type AccountBalance,
  type OrderSide,
  type OrderType,
  type TradingPair,
} from '../api';
import { describeError, describeReason } from '../errors';
import {
  MARKET_BUY_BUFFER_DENOMINATOR,
  MARKET_BUY_BUFFER_NUMERATOR,
  formatInputValue,
  formatPrice,
  formatQuantity,
  formatUnits,
  marketBuyAmount,
  parseUnits,
  quoteAmount,
  toSafeNumber,
} from '../format';
import type { ToastKind } from './ToastStack';

interface OrderFormProps {
  pair?: TradingPair;
  userId: number;
  balances: AccountBalance[];
  currencyScales: Record<string, number>;
  lastPrice?: number;
  /** 卖一价（价格最小单位）；市价买优先用它，没有时回退到 lastPrice。 */
  bestAsk?: number;
  onOrderPlaced: () => void;
  /** 余额不足时主动刷新余额快照，避免轮询滞后导致误判。 */
  onRefreshBalances: () => void;
  notify: (kind: ToastKind, text: string) => void;
}

interface OrderResult {
  kind: 'success' | 'error';
  title: string;
  detail: string;
  code?: string;
}

function sanitizeDecimal(value: string): string {
  let cleaned = value.replace(/[^\d.]/g, '');
  const dot = cleaned.indexOf('.');
  if (dot !== -1) {
    cleaned = `${cleaned.slice(0, dot + 1)}${cleaned.slice(dot + 1).replace(/\./g, '')}`;
  }
  return cleaned;
}

function feePercent(bps: number): string {
  return `${(bps / 100).toFixed(2)}%`;
}

export function OrderForm({
  pair,
  userId,
  balances,
  currencyScales,
  lastPrice,
  bestAsk,
  onOrderPlaced,
  onRefreshBalances,
  notify,
}: OrderFormProps) {
  const [side, setSide] = useState<OrderSide>('BUY');
  const [type, setType] = useState<OrderType>('LIMIT');
  const [priceInput, setPriceInput] = useState('');
  const [quantityInput, setQuantityInput] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<OrderResult | null>(null);
  const requestIdRef = useRef<string | null>(null);
  const priceTouchedRef = useRef(false);

  const symbol = pair?.symbol;

  useEffect(() => {
    priceTouchedRef.current = false;
    requestIdRef.current = null;
    setQuantityInput('');
    setResult(null);
    setPriceInput(
      pair && lastPrice !== undefined ? formatInputValue(lastPrice, pair.priceScale) : '',
    );
    // 只在新交易对时重置表单；lastPrice 变化由下面的 effect 处理。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol]);

  useEffect(() => {
    if (!pair || priceTouchedRef.current || lastPrice === undefined) {
      return;
    }
    setPriceInput(formatInputValue(lastPrice, pair.priceScale));
  }, [lastPrice, pair]);

  const baseBalance = pair
    ? (balances.find((balance) => balance.currency === pair.baseCurrency)?.available ?? 0)
    : 0;
  const quoteBalance = pair
    ? (balances.find((balance) => balance.currency === pair.quoteCurrency)?.available ?? 0)
    : 0;
  const baseScale = pair ? (currencyScales[pair.baseCurrency] ?? pair.quantityScale) : 0;
  const quoteScale = pair ? (currencyScales[pair.quoteCurrency] ?? 0) : 0;

  const isMarketBuy = side === 'BUY' && type === 'MARKET';
  // 市价买入参考价：优先卖一价，没有卖一价时回退到最新成交价。
  const referencePrice = isMarketBuy
    ? (bestAsk !== undefined && bestAsk > 0 ? bestAsk : lastPrice)
    : undefined;
  const hasReferencePrice =
    referencePrice !== undefined && Number.isFinite(referencePrice) && referencePrice > 0;

  const priceRaw = pair && type === 'LIMIT' ? parseUnits(priceInput, pair.priceScale) : 0n;
  const quantityRaw = pair ? parseUnits(quantityInput, pair.quantityScale) : null;
  const priceInvalid = type === 'LIMIT' && priceInput.trim() !== '' && priceRaw === null;
  const quantityInvalid = quantityInput.trim() !== '' && quantityRaw === null;

  // 市价买入冻结预算 = 参考价 × 1.05 × 数量（计价币最小单位）。
  const marketAmount = useMemo(() => {
    if (!pair || !isMarketBuy || quantityRaw === null || quantityRaw <= 0n) {
      return null;
    }
    if (!hasReferencePrice || referencePrice === undefined) {
      return null;
    }
    return marketBuyAmount(
      BigInt(Math.trunc(referencePrice)),
      quantityRaw,
      pair.priceScale,
      pair.quantityScale,
      quoteScale,
    );
  }, [pair, isMarketBuy, quantityRaw, hasReferencePrice, referencePrice, quoteScale]);

  const availableRaw = side === 'BUY' ? quoteBalance : baseBalance;
  const availableCurrency = pair ? (side === 'BUY' ? pair.quoteCurrency : pair.baseCurrency) : '';
  const availableScale = side === 'BUY' ? quoteScale : baseScale;

  const freeze = useMemo(() => {
    if (!pair || quantityRaw === null || quantityRaw <= 0n) {
      return null;
    }
    if (side === 'SELL') {
      return {
        raw: quantityRaw,
        currency: pair.baseCurrency,
        scale: baseScale,
      };
    }
    if (type === 'MARKET') {
      return marketAmount === null || marketAmount <= 0n
        ? null
        : {
            raw: marketAmount,
            currency: pair.quoteCurrency,
            scale: quoteScale,
          };
    }
    if (priceRaw === null || priceRaw <= 0n) {
      return null;
    }
    return {
      raw: quoteAmount(
        priceRaw,
        quantityRaw,
        pair.priceScale,
        pair.quantityScale,
        quoteScale,
      ),
      currency: pair.quoteCurrency,
      scale: quoteScale,
    };
  }, [pair, quantityRaw, side, type, priceRaw, baseScale, quoteScale, marketAmount]);

  // 前端余额校验：限价买 / 市价买按冻结金额比可用计价币，卖单按数量比可用基础币。
  // 余额快照每 5 秒轮询，可能短暂滞后；提交时若判定不足会主动刷新一次再让用户确认。
  const availableRawBig = BigInt(Math.trunc(availableRaw));
  const balanceInsufficient = freeze !== null && freeze.raw > availableRawBig;
  const balanceShortfall = balanceInsufficient ? freeze.raw - availableRawBig : null;

  const changePrice = (value: string) => {
    priceTouchedRef.current = true;
    requestIdRef.current = null;
    setPriceInput(sanitizeDecimal(value));
  };

  const changeQuantity = (value: string) => {
    requestIdRef.current = null;
    setQuantityInput(sanitizeDecimal(value));
  };

  const changeSide = (next: OrderSide) => {
    requestIdRef.current = null;
    setSide(next);
  };

  const changeType = (next: OrderType) => {
    requestIdRef.current = null;
    setType(next);
  };

  const applyPercent = (percent: number) => {
    if (!pair) {
      return;
    }
    requestIdRef.current = null;
    if (side === 'SELL') {
      const quantity = (BigInt(baseBalance) * BigInt(percent)) / 100n;
      setQuantityInput(formatInputValue(quantity, pair.quantityScale));
      return;
    }
    if (type === 'MARKET') {
      if (!hasReferencePrice || referencePrice === undefined) {
        notify('info', '暂无卖一价或最新成交价，无法估算市价买入数量');
        return;
      }
      // quantity = 可用金额 / (参考价 × 1.05)
      const budget = (BigInt(quoteBalance) * BigInt(percent)) / 100n;
      const numerator =
        budget *
        MARKET_BUY_BUFFER_DENOMINATOR *
        10n ** BigInt(pair.priceScale + pair.quantityScale);
      const denominator =
        BigInt(Math.trunc(referencePrice)) *
        MARKET_BUY_BUFFER_NUMERATOR *
        10n ** BigInt(quoteScale);
      const quantity = numerator / denominator;
      if (quantity <= 0n) {
        notify('info', '可用余额不足');
        return;
      }
      setQuantityInput(formatInputValue(quantity, pair.quantityScale));
      return;
    }
    if (priceRaw === null || priceRaw <= 0n) {
      notify('info', '买入前请先填写有效的限价');
      return;
    }
    const budget = (BigInt(quoteBalance) * BigInt(percent)) / 100n;
    setQuantityInput(formatInputValue(budget / priceRaw, pair.quantityScale));
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!pair) {
      notify('error', '请先选择交易对');
      return;
    }

    const parsedPrice = type === 'LIMIT' ? parseUnits(priceInput, pair.priceScale) : 0n;
    const parsedQuantity = parseUnits(quantityInput, pair.quantityScale);

    if (type === 'LIMIT' && (parsedPrice === null || parsedPrice <= 0n)) {
      const message = `价格必须是大于 0 的数字，最多 ${pair.priceScale} 位小数`;
      setResult({ kind: 'error', title: '下单失败', detail: message, code: 'PRICE_REQUIRED_FOR_LIMIT' });
      notify('error', message);
      return;
    }
    if (parsedQuantity === null || parsedQuantity <= 0n) {
      const message = `数量必须是大于 0 的数字，最多 ${pair.quantityScale} 位小数`;
      setResult({ kind: 'error', title: '下单失败', detail: message, code: 'QUANTITY_INVALID' });
      notify('error', message);
      return;
    }
    if (pair.minOrderQuantity > 0 && parsedQuantity < BigInt(pair.minOrderQuantity)) {
      const message = `低于最小下单量 ${formatQuantity(
        pair.minOrderQuantity,
        pair.quantityScale,
      )} ${pair.baseCurrency}`;
      setResult({ kind: 'error', title: '下单失败', detail: message, code: 'BELOW_MIN_ORDER_QUANTITY' });
      notify('error', message);
      return;
    }
    if (isMarketBuy) {
      if (!hasReferencePrice || marketAmount === null || marketAmount <= 0n) {
        const message = '暂无卖一价或最新成交价，无法计算市价买入金额';
        setResult({
          kind: 'error',
          title: '下单失败',
          detail: message,
          code: 'MARKET_PRICE_UNAVAILABLE',
        });
        notify('error', message);
        return;
      }
      if (pair.minOrderAmount > 0 && marketAmount < BigInt(pair.minOrderAmount)) {
        const message = `低于最小下单金额 ${formatUnits(
          pair.minOrderAmount,
          quoteScale,
        )} ${pair.quoteCurrency}`;
        setResult({ kind: 'error', title: '下单失败', detail: message, code: 'BELOW_MIN_ORDER_AMOUNT' });
        notify('error', message);
        return;
      }
    }
    if (
      type === 'LIMIT' &&
      parsedPrice !== null &&
      parsedPrice > 0n &&
      pair.minOrderAmount > 0 &&
      quoteAmount(
        parsedPrice,
        parsedQuantity,
        pair.priceScale,
        pair.quantityScale,
        quoteScale,
      ) < BigInt(pair.minOrderAmount)
    ) {
      const message = `低于最小下单金额 ${formatUnits(
        pair.minOrderAmount,
        quoteScale,
      )} ${pair.quoteCurrency}`;
      setResult({ kind: 'error', title: '下单失败', detail: message, code: 'BELOW_MIN_ORDER_AMOUNT' });
      notify('error', message);
      return;
    }
    if (freeze !== null && freeze.raw > availableRawBig) {
      const shortfall = freeze.raw - availableRawBig;
      const message = `可用余额不足：需要 ${formatUnits(
        freeze.raw,
        freeze.scale,
      )} ${freeze.currency}，当前可用 ${formatUnits(
        availableRaw,
        availableScale,
      )} ${freeze.currency}，还差 ${formatUnits(shortfall, freeze.scale)} ${freeze.currency}。`;
      setResult({
        kind: 'error',
        title: '余额不足',
        detail: `${message}余额快照可能有延迟，已为你刷新，请确认后重试。`,
        code: 'INSUFFICIENT_AVAILABLE_BALANCE',
      });
      notify('error', message);
      // balances 是轮询快照，可能刚充值/成交后滞后；主动刷新一次再让用户重试。
      onRefreshBalances();
      return;
    }

    const quantityNumber = toSafeNumber(parsedQuantity);
    const priceNumber = type === 'LIMIT' ? toSafeNumber(parsedPrice ?? 0n) : 0;
    const amountNumber = isMarketBuy && marketAmount !== null
      ? toSafeNumber(marketAmount)
      : null;
    if (
      quantityNumber === null ||
      priceNumber === null ||
      (isMarketBuy && amountNumber === null)
    ) {
      const message = '下单数量或金额超出前端可精确表示的范围';
      setResult({ kind: 'error', title: '下单失败', detail: message, code: 'AMOUNT_OVERFLOW' });
      notify('error', message);
      return;
    }

    if (requestIdRef.current === null) {
      requestIdRef.current = crypto.randomUUID();
    }
    const requestId = requestIdRef.current;

    setSubmitting(true);
    try {
      const response = await placeOrder({
        requestId,
        userId,
        symbol: pair.symbol,
        side,
        type,
        price: priceNumber,
        quantity: quantityNumber,
        amount: isMarketBuy && amountNumber !== null ? amountNumber : undefined,
      });

      if (!response.success || response.orderId === null) {
        throw new ApiError(response.reason ?? 'ORDER_REJECTED', 400, response);
      }

      requestIdRef.current = null;
      const title = `委托命令已提交 · #${response.orderId}`;
      const detail = '下单命令已写入 Kafka，等待资金冻结与撮合结果';
      setResult({ kind: 'success', title, detail });
      notify('success', `委托命令已提交：${side === 'BUY' ? '买入' : '卖出'} ${formatQuantity(
        quantityNumber,
        pair.quantityScale,
      )} ${pair.baseCurrency}`);
      onOrderPlaced();
    } catch (error) {
      const message = describeError(error);
      const code = error instanceof ApiError ? error.reason : undefined;
      setResult({
        kind: 'error',
        title: '下单失败',
        detail:
          code === 'ORDER_COMMAND_PUBLISH_FAILED'
            ? `${describeReason(code)}。点击提交会复用同一个 requestId 重试。`
            : message,
        code,
      });
      notify('error', code ? describeReason(code) : message);
      if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
        requestIdRef.current = null;
      }
    } finally {
      setSubmitting(false);
    }
  };

  const submitDisabled =
    !pair ||
    submitting ||
    (isMarketBuy && !hasReferencePrice) ||
    priceInvalid ||
    quantityInvalid;

  return (
    <section className="panel order-panel">
      <div className="order-tabs" role="tablist" aria-label="买入或卖出">
        <button
          type="button"
          role="tab"
          aria-selected={side === 'BUY'}
          className={`order-tab buy ${side === 'BUY' ? 'active' : ''}`}
          onClick={() => changeSide('BUY')}
        >
          买入
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={side === 'SELL'}
          className={`order-tab sell ${side === 'SELL' ? 'active' : ''}`}
          onClick={() => changeSide('SELL')}
        >
          卖出
        </button>
      </div>

      <div className="type-row">
        <div className="type-switch" role="group" aria-label="委托类型">
          <button
            type="button"
            className={type === 'LIMIT' ? 'active' : ''}
            onClick={() => changeType('LIMIT')}
          >
            限价
          </button>
          <button
            type="button"
            className={type === 'MARKET' ? 'active' : ''}
            onClick={() => changeType('MARKET')}
          >
            市价
          </button>
        </div>
        {pair && (
          <span className="fee-hint">
            Maker {feePercent(pair.makerFeeBps)} · Taker {feePercent(pair.takerFeeBps)}
          </span>
        )}
      </div>

      <form className="order-form" onSubmit={handleSubmit}>
        <label className={`field ${priceInvalid ? 'invalid' : ''}`}>
          <span className="field-label">价格</span>
          <div className="input-wrap">
            <input
              value={
                type === 'MARKET' && pair && hasReferencePrice && referencePrice !== undefined
                  ? formatInputValue(BigInt(Math.trunc(referencePrice)), pair.priceScale)
                  : priceInput
              }
              onChange={(event) => changePrice(event.target.value)}
              inputMode="decimal"
              placeholder={
                type === 'MARKET'
                  ? hasReferencePrice
                    ? '参考价'
                    : '暂无参考价'
                  : '0.00'
              }
              disabled={type === 'MARKET' || !pair}
              aria-label="委托价格"
            />
            <span className="input-suffix">{pair?.quoteCurrency ?? '—'}</span>
          </div>
        </label>

        <label className={`field ${quantityInvalid ? 'invalid' : ''}`}>
          <span className="field-label">数量</span>
          <div className="input-wrap">
            <input
              value={quantityInput}
              onChange={(event) => changeQuantity(event.target.value)}
              inputMode="decimal"
              placeholder="0.00"
              disabled={!pair}
              aria-label="委托数量"
            />
            <span className="input-suffix">{pair?.baseCurrency ?? '—'}</span>
          </div>
        </label>

        <div className="percent-row">
          {[25, 50, 75, 100].map((percent) => (
            <button
              key={percent}
              type="button"
              onClick={() => applyPercent(percent)}
              disabled={!pair || submitting}
            >
              {percent}%
            </button>
          ))}
        </div>

        <div className="order-info-grid">
          <div>
            <span>可用</span>
            <b className={balanceInsufficient ? 'balance-insufficient' : undefined}>
              {formatUnits(availableRaw, availableScale)} {availableCurrency}
            </b>
          </div>
          <div>
            <span
              title={
                isMarketBuy
                  ? '参考价 × 1.05 × 数量；撮合按实际卖价扣款，差额退回'
                  : '后端账本口径：price × quantity 的最小单位乘积'
              }
            >
              账本冻结
            </span>
            <b title="按后端账本最小单位换算">
              {freeze
                ? `${formatUnits(freeze.raw, freeze.scale)} ${freeze.currency}`
                : '--'}
            </b>
          </div>
        </div>

        {pair && (pair.minOrderQuantity > 0 || pair.minOrderAmount > 0) && (
          <p className="min-hint">
            最小下单量 {formatQuantity(pair.minOrderQuantity, pair.quantityScale)}{' '}
            {pair.baseCurrency} · 最小金额{' '}
            {formatUnits(pair.minOrderAmount, quoteScale)} {pair.quoteCurrency}
          </p>
        )}

        {isMarketBuy && pair && (
          <p className={hasReferencePrice ? 'min-hint' : 'form-warning'}>
            {hasReferencePrice && referencePrice !== undefined
              ? `参考${bestAsk !== undefined && bestAsk > 0 ? '卖一价' : '最新成交价'} ${formatPrice(
                  BigInt(Math.trunc(referencePrice)),
                  pair.priceScale,
                )} ${pair.quoteCurrency}，按 1.05 倍预估冻结；撮合后未用完的金额退回可用余额。`
              : '暂无卖一价与最新成交价，暂时无法计算市价买入金额。'}
          </p>
        )}

        {balanceInsufficient && balanceShortfall !== null && freeze && (
          <p className="form-warning">
            可用余额不足：需要 {formatUnits(freeze.raw, freeze.scale)} {freeze.currency}，
            当前可用 {formatUnits(availableRaw, availableScale)} {availableCurrency}，
            还差 {formatUnits(balanceShortfall, freeze.scale)} {freeze.currency}。
          </p>
        )}

        <button
          type="submit"
          className={`submit-button ${side === 'BUY' ? 'buy' : 'sell'}`}
          disabled={submitDisabled}
        >
          {submitting
            ? '提交中…'
            : side === 'BUY'
              ? `买入 ${pair?.baseCurrency ?? ''}`
              : `卖出 ${pair?.baseCurrency ?? ''}`}
        </button>

        <p className="idempotency-hint">
          提交时自动生成 requestId；网络异常重试会复用同一个幂等键。
        </p>
      </form>

      {result && (
        <div className={`order-result ${result.kind}`}>
          <div className="result-head">
            <strong>{result.title}</strong>
            {result.code && <code>{result.code}</code>}
          </div>
          <p>{result.detail}</p>
        </div>
      )}
    </section>
  );
}
