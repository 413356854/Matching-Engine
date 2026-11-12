import { useState } from 'react';
import type { OrderBookDepth, Trade, TradingPair } from '../api';
import { LatestTradesPanel } from './LatestTradesPanel';
import { OrderBookPanel } from './OrderBookPanel';

interface BookTradesPanelProps {
  pair?: TradingPair;
  orderBook: OrderBookDepth | null;
  orderBookError: string | null;
  trades: Trade[];
  tradesError: string | null;
}

type BookTradesTab = 'book' | 'trades';

export function BookTradesPanel({
  pair,
  orderBook,
  orderBookError,
  trades,
  tradesError,
}: BookTradesPanelProps) {
  const [tab, setTab] = useState<BookTradesTab>('book');

  return (
    <section className="panel book-trades-panel">
      <div className="panel-header book-trades-header">
        <div className="book-trades-tabs" role="tablist" aria-label="订单簿与最新成交">
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'book'}
            className={`book-trades-tab ${tab === 'book' ? 'active' : ''}`}
            onClick={() => setTab('book')}
            onMouseEnter={() => setTab('book')}
          >
            订单簿
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'trades'}
            className={`book-trades-tab ${tab === 'trades' ? 'active' : ''}`}
            onClick={() => setTab('trades')}
            onMouseEnter={() => setTab('trades')}
          >
            最新成交
          </button>
        </div>

        <div className="book-trades-meta">
          <span className="refresh-badge">
            <span className="live-dot" />
            3s
          </span>
        </div>
      </div>

      {tab === 'book' ? (
        <OrderBookPanel
          pair={pair}
          orderBook={orderBook}
          lastTrade={trades[0]}
          error={orderBookError}
          embedded
        />
      ) : (
        <LatestTradesPanel pair={pair} trades={trades} error={tradesError} embedded />
      )}
    </section>
  );
}
