import type { AccountBalance, TradingPair } from '../api';
import { formatDisplayUnits } from '../format';
import { RefreshIcon, WalletIcon } from './icons';

interface BalancesPanelProps {
  userId: number;
  balances: AccountBalance[];
  pair?: TradingPair;
  currencyScales: Record<string, number>;
  currencyDisplayScales: Record<string, number>;
  error: string | null;
  onRefresh: () => void;
}

export function BalancesPanel({
  userId,
  balances,
  pair,
  currencyScales,
  currencyDisplayScales,
  error,
  onRefresh,
}: BalancesPanelProps) {
  const priority = (currency: string) => {
    if (pair && currency === pair.baseCurrency) {
      return 0;
    }
    if (pair && currency === pair.quoteCurrency) {
      return 1;
    }
    return 2;
  };

  const ordered = [...balances].sort((a, b) => {
    const diff = priority(a.currency) - priority(b.currency);
    return diff !== 0 ? diff : a.currency.localeCompare(b.currency);
  });

  return (
    <section className="panel balances-panel">
      <div className="panel-header balances-header">
        <div className="balances-title">
          <WalletIcon />
          <div>
            <h2>资产</h2>
            <span className="panel-sub">用户 {userId} · 手动刷新</span>
          </div>
        </div>
        <button
          type="button"
          className="balance-refresh"
          onClick={onRefresh}
          title="立即刷新余额"
        >
          <RefreshIcon />
          刷新余额
        </button>
      </div>

      <div className="balance-columns">
        <span aria-hidden="true" />
        <span>币种</span>
        <span className="align-right">可用</span>
        <span className="align-right">冻结</span>
      </div>

      <div className="balance-list">
        {ordered.map((balance) => {
          const rawScale = currencyScales[balance.currency] ?? 0;
          const displayScale = currencyDisplayScales[balance.currency] ?? rawScale;
          const isPairCurrency =
            pair &&
            (balance.currency === pair.baseCurrency || balance.currency === pair.quoteCurrency);
          return (
            <div key={balance.currency} className="balance-row">
              <span className={`currency-icon ${isPairCurrency ? 'highlight' : ''}`}>
                {balance.currency.slice(0, 1)}
              </span>
              <span className="balance-currency">
                <b>{balance.currency}</b>
                <small
                  title={`可用 ${balance.available} / 冻结 ${balance.frozen}（最小单位）`}
                >
                  seq {balance.seq}
                </small>
              </span>
              <span className="balance-value" title={`${balance.available} 最小单位`}>
                {formatDisplayUnits(balance.available, rawScale, displayScale)}
              </span>
              <span className="balance-value frozen" title={`${balance.frozen} 最小单位`}>
                {formatDisplayUnits(balance.frozen, rawScale, displayScale)}
              </span>
            </div>
          );
        })}

        {ordered.length === 0 && (
          <div className="empty-hint balance-empty">
            {error ? error : '该用户暂无资产。可通过 Kafka me.account.commands 充值后刷新。'}
          </div>
        )}
      </div>
    </section>
  );
}
