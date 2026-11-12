import { useEffect, useMemo, useRef, useState, type MouseEvent } from 'react';
import type { TradingPair } from '../api';
import { formatPrice } from '../format';
import { SearchIcon } from './icons';

interface MarketPickerModalProps {
  open: boolean;
  pairs: TradingPair[];
  selectedSymbol: string;
  lastPrices: Record<string, number>;
  onSelect: (symbol: string) => void;
  onClose: () => void;
}

export function MarketPickerModal({
  open,
  pairs,
  selectedSymbol,
  lastPrices,
  onSelect,
  onClose,
}: MarketPickerModalProps) {
  const [query, setQuery] = useState('');
  const searchRef = useRef<HTMLInputElement>(null);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  });

  useEffect(() => {
    if (!open) {
      return;
    }
    setQuery('');
    const focusTimer = window.setTimeout(() => searchRef.current?.focus(), 60);
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onCloseRef.current();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.clearTimeout(focusTimer);
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [open]);

  const visiblePairs = useMemo(() => {
    const keyword = query.trim().toUpperCase();
    if (!keyword) {
      return pairs;
    }
    return pairs.filter(
      (pair) =>
        pair.symbol.toUpperCase().includes(keyword) ||
        pair.baseCurrency.toUpperCase().includes(keyword),
    );
  }, [pairs, query]);

  if (!open) {
    return null;
  }

  const handleBackdrop = (event: MouseEvent<HTMLDivElement>) => {
    if (event.target === event.currentTarget) {
      onClose();
    }
  };

  const choose = (symbol: string) => {
    onSelect(symbol);
    onClose();
  };

  return (
    <div className="modal-backdrop" onMouseDown={handleBackdrop}>
      <div
        className="modal-card market-picker-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby="market-picker-title"
      >
        <div className="modal-head">
          <div>
            <h2 id="market-picker-title">选择交易对</h2>
            <p className="modal-sub">{pairs.length} 个市场 · 点击切换</p>
          </div>
          <button type="button" className="modal-close" onClick={onClose} aria-label="关闭">
            ×
          </button>
        </div>

        <div className="market-picker-body">
          <label className="search-box">
            <SearchIcon />
            <input
              ref={searchRef}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="搜索 BTC / USDT"
              aria-label="搜索交易对"
            />
          </label>

          <div className="market-list" role="list">
            {visiblePairs.map((pair) => {
              const price = lastPrices[pair.symbol];
              const active = pair.symbol === selectedSymbol;
              return (
                <button
                  key={pair.symbol}
                  type="button"
                  role="listitem"
                  className={`market-row ${active ? 'active' : ''}`}
                  onClick={() => choose(pair.symbol)}
                >
                  <span className="market-symbol">
                    <b>{pair.baseCurrency}</b>
                    <span>/{pair.quoteCurrency}</span>
                  </span>
                  <span className="market-price">
                    {price === undefined ? '--' : formatPrice(price, pair.priceScale)}
                    <small>{pair.quoteCurrency}</small>
                  </span>
                </button>
              );
            })}

            {visiblePairs.length === 0 && (
              <div className="empty-hint">
                {pairs.length === 0 ? '正在加载交易对…' : '没有匹配的交易对'}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
