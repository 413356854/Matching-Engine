import { useEffect, useMemo, useRef, useState, type FormEvent, type MouseEvent } from 'react';
import { ApiError, rechargeAccount, type Currency } from '../api';
import { describeError } from '../errors';

export interface RechargeSuccess {
  requestId: string;
  userId: number;
  currency: string;
  displayAmount: number;
  ledgerAmount: number;
}

interface RechargeModalProps {
  open: boolean;
  currentUserId: number;
  currencies: Currency[];
  currencyScales: Record<string, number>;
  defaultCurrency: string;
  onClose: () => void;
  /** 充值提交成功：切换到充值的用户，弹框保持打开以便继续充值其他币种。 */
  onRechargeSuccess: (result: RechargeSuccess) => void;
  /** 登录：切换到输入的用户，不产生充值。 */
  onLogin: (userId: number) => void;
}

const LONG_MAX = 9_223_372_036_854_775_807n;
const DEFAULT_AMOUNT = '1000000';

const FALLBACK_CURRENCIES: Currency[] = [
  { id: -1, code: 'USDT', name: '泰达币', scale: 6, status: 1, sortOrder: 1 },
  { id: -2, code: 'BTC', name: '比特币', scale: 8, status: 1, sortOrder: 2 },
  { id: -3, code: 'ETH', name: '以太坊', scale: 8, status: 1, sortOrder: 3 },
];

function groupDigits(value: number): string {
  return value.toLocaleString('zh-CN', { maximumFractionDigits: 0 });
}

function digitsOnly(value: string): string {
  return value.replace(/[^\d]/g, '');
}

export function RechargeModal({
  open,
  currentUserId,
  currencies,
  currencyScales,
  defaultCurrency,
  onClose,
  onRechargeSuccess,
  onLogin,
}: RechargeModalProps) {
  const [userIdInput, setUserIdInput] = useState(String(currentUserId));
  const [currency, setCurrency] = useState(defaultCurrency);
  const [amountInput, setAmountInput] = useState(DEFAULT_AMOUNT);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const userInputRef = useRef<HTMLInputElement>(null);

  const options = useMemo(() => {
    if (currencies.length === 0) {
      return FALLBACK_CURRENCIES;
    }
    return [...currencies].sort((left, right) => left.sortOrder - right.sortOrder);
  }, [currencies]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const preferred = options.some((item) => item.code === defaultCurrency)
      ? defaultCurrency
      : (options[0]?.code ?? 'USDT');
    setUserIdInput(String(currentUserId));
    setCurrency(preferred);
    setAmountInput(DEFAULT_AMOUNT);
    setError(null);
    setSubmitting(false);
    const focusTimer = window.setTimeout(() => userInputRef.current?.focus(), 60);
    return () => window.clearTimeout(focusTimer);
    // 只在打开弹框时重置表单；轮询更新 currencies 不应打断用户输入。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (!open) {
      return;
    }
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
  }, [onClose, open, submitting]);

  if (!open) {
    return null;
  }

  const scale = currencyScales[currency] ?? options.find((item) => item.code === currency)?.scale ?? 6;
  const userId = Number(userIdInput);
  const validUserId = Number.isSafeInteger(userId) && userId > 0;
  const displayAmount = Number(amountInput);
  const validAmount = Number.isSafeInteger(displayAmount) && displayAmount > 0;
  const ledgerAmount =
    validAmount && Number.isSafeInteger(scale)
      ? BigInt(displayAmount) * 10n ** BigInt(scale)
      : null;
  const ledgerOverflow = ledgerAmount !== null && ledgerAmount > LONG_MAX;

  const handleBackdrop = (event: MouseEvent<HTMLDivElement>) => {
    if (event.target === event.currentTarget && !submitting) {
      onClose();
    }
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!validUserId) {
      setError('用户 ID 必须是正整数');
      return;
    }
    if (!validAmount) {
      setError('充值金额必须是大于 0 的整数');
      return;
    }
    if (ledgerAmount === null || ledgerOverflow) {
      setError('充值金额超出可表示范围');
      return;
    }

    setSubmitting(true);
    setError(null);
    try {
      const response = await rechargeAccount(userId, currency, displayAmount);
      if (!response.success || !response.requestId) {
        throw new ApiError(response.reason ?? 'RECHARGE_PUBLISH_FAILED', 400, response);
      }
      onRechargeSuccess({
        requestId: response.requestId,
        userId: response.userId,
        currency: response.currency,
        displayAmount: response.displayAmount,
        ledgerAmount: response.amount,
      });
    } catch (submitError) {
      setError(describeError(submitError));
    } finally {
      setSubmitting(false);
    }
  };

  /** 只切换当前登录用户，不产生充值。 */
  const handleLogin = () => {
    if (!validUserId) {
      setError('用户 ID 必须是正整数');
      return;
    }
    setError(null);
    onLogin(userId);
  };

  return (
    <div className="modal-backdrop" onMouseDown={handleBackdrop}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby="recharge-modal-title"
      >
        <div className="modal-head">
          <div>
            <h2 id="recharge-modal-title">模拟登录 / 充值</h2>
            <p className="modal-sub">确认充值会为目标用户入账并切换为当前用户；登录只切换用户，不产生充值。</p>
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
          <label className="modal-field">
            <span>用户 ID</span>
            <input
              ref={userInputRef}
              value={userIdInput}
              onChange={(event) => setUserIdInput(digitsOnly(event.target.value))}
              inputMode="numeric"
              placeholder="1001"
              maxLength={18}
              autoComplete="off"
            />
          </label>

          <label className="modal-field">
            <span>币种</span>
            <select value={currency} onChange={(event) => setCurrency(event.target.value)}>
              {options.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.code} · {item.name}（精度 {item.scale}）
                </option>
              ))}
            </select>
          </label>

          <label className="modal-field">
            <span>
              充值金额 <em>默认 100 万</em>
            </span>
            <div className="modal-amount">
              <input
                value={amountInput}
                onChange={(event) => setAmountInput(digitsOnly(event.target.value))}
                inputMode="numeric"
                placeholder={DEFAULT_AMOUNT}
                maxLength={15}
                autoComplete="off"
              />
              <span>{currency}</span>
            </div>
          </label>

          <div className="modal-summary">
            <span>展示金额</span>
            <b>
              {validAmount ? groupDigits(displayAmount) : '--'} {currency}
            </b>
          </div>
          <div className="modal-summary">
            <span>账本入账</span>
            <b className={ledgerOverflow ? 'down' : undefined}>
              {ledgerAmount === null
                ? '--'
                : ledgerOverflow
                  ? '超出范围'
                  : ledgerAmount.toLocaleString('zh-CN')}{' '}
              <small>最小单位</small>
            </b>
          </div>

          <p className="modal-hint">
            金额是展示单位的整数，后端按 {currency} 精度 {scale} 换算成最小单位后，
            通过 Kafka <code>me.account.commands</code> 异步入账；充值成功后自动切换到该用户，
            弹框保持打开，可继续选择其他币种充值。只想切换用户、不充值时使用「登录」。
            演示接口无鉴权。
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
            <button
              type="button"
              className="modal-login"
              onClick={handleLogin}
              disabled={submitting}
            >
              登录
            </button>
            <button type="submit" className="modal-submit" disabled={submitting}>
              {submitting ? '提交中…' : '确认充值'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
