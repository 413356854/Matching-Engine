import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  fetchBalances,
  fetchCurrencies,
  fetchKlines,
  fetchOpenOrders,
  fetchOrderBook,
  fetchPairs,
  fetchTrades,
  fetchUserFundsFlows,
  isAbortError,
  type AccountBalance,
  type Currency,
  type FundsFlow,
  type Kline,
  type OrderBookDepth,
  type OrderView,
  type Trade,
  type TradingPair,
} from './api';
import { AccountActivityPanel } from './components/AccountActivityPanel';
import { AdminPage } from './components/AdminPage';
import { BalancesPanel } from './components/BalancesPanel';
import { BookTradesPanel } from './components/BookTradesPanel';
import { ChartPanel } from './components/ChartPanel';
import { Header, type AppView } from './components/Header';
import { MarketPickerModal } from './components/MarketPickerModal';
import { OrderForm } from './components/OrderForm';
import { RechargeModal, type RechargeSuccess } from './components/RechargeModal';
import { ToastStack, type ToastItem, type ToastKind } from './components/ToastStack';
import { describeError } from './errors';
import { usePolling } from './hooks/usePolling';

const USER_KEY = 'me.userId';
const SYMBOL_KEY = 'me.symbol';
const DEFAULT_USER_ID = 1001;

function storedUserId(): number {
  const value = Number(localStorage.getItem(USER_KEY));
  return Number.isInteger(value) && value > 0 ? value : DEFAULT_USER_ID;
}

function App() {
  const [userId, setUserId] = useState(storedUserId);
  const [pairs, setPairs] = useState<TradingPair[]>([]);
  const [currencies, setCurrencies] = useState<Currency[]>([]);
  const [balances, setBalances] = useState<AccountBalance[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState(
    () => localStorage.getItem(SYMBOL_KEY) ?? '',
  );
  const [klines, setKlines] = useState<Kline[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [openOrders, setOpenOrders] = useState<OrderView[]>([]);
  const [fundsFlows, setFundsFlows] = useState<FundsFlow[]>([]);
  const [orderBook, setOrderBook] = useState<OrderBookDepth | null>(null);
  const [lastPrices, setLastPrices] = useState<Record<string, number>>({});
  const [online, setOnline] = useState<boolean | null>(null);
  const [pairsError, setPairsError] = useState<string | null>(null);
  const [balancesError, setBalancesError] = useState<string | null>(null);
  const [marketError, setMarketError] = useState<string | null>(null);
  const [orderBookError, setOrderBookError] = useState<string | null>(null);
  const [ordersError, setOrdersError] = useState<string | null>(null);
  const [fundsFlowsError, setFundsFlowsError] = useState<string | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [balancesNonce, setBalancesNonce] = useState(0);
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const [loginOpen, setLoginOpen] = useState(false);
  const [marketPickerOpen, setMarketPickerOpen] = useState(false);
  const [view, setView] = useState<AppView>('trade');

  const toastTimers = useRef(new Map<number, number>());
  const refreshTimer = useRef<number | null>(null);

  const dismissToast = useCallback((id: number) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
    const timer = toastTimers.current.get(id);
    if (timer !== undefined) {
      window.clearTimeout(timer);
      toastTimers.current.delete(id);
    }
  }, []);

  const notify = useCallback((kind: ToastKind, text: string) => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setToasts((current) => [...current.slice(-3), { id, kind, text }]);
    const timer = window.setTimeout(() => {
      setToasts((current) => current.filter((toast) => toast.id !== id));
      toastTimers.current.delete(id);
    }, 4200);
    toastTimers.current.set(id, timer);
  }, []);

  useEffect(
    () => () => {
      toastTimers.current.forEach((timer) => window.clearTimeout(timer));
      if (refreshTimer.current !== null) {
        window.clearTimeout(refreshTimer.current);
      }
    },
    [],
  );

  const refreshAll = useCallback(() => {
    setRefreshNonce((value) => value + 1);
  }, []);

  const refreshBalances = useCallback(() => {
    setBalancesNonce((value) => value + 1);
  }, []);

  const handleOrderPlaced = useCallback(() => {
    refreshAll();
    if (refreshTimer.current !== null) {
      window.clearTimeout(refreshTimer.current);
    }
    refreshTimer.current = window.setTimeout(refreshAll, 1500);
  }, [refreshAll]);

  const handleRechargeSuccess = useCallback(
    (result: RechargeSuccess) => {
      // 充值成功后直接切换到目标用户，避免“充了 A、却用 B 下单”导致的余额不足；
      // 弹框保持打开，方便连续为同一用户充值其他币种。
      setUserId(result.userId);
      localStorage.setItem(USER_KEY, String(result.userId));
      refreshAll();
      if (refreshTimer.current !== null) {
        window.clearTimeout(refreshTimer.current);
      }
      refreshTimer.current = window.setTimeout(refreshAll, 1200);
      notify(
        'success',
        `已为用户 ${result.userId} 提交 ${result.displayAmount.toLocaleString(
          'zh-CN',
        )} ${result.currency} 充值，已切换为当前用户`,
      );
    },
    [notify, refreshAll],
  );

  const handleLogin = useCallback(
    (nextUserId: number) => {
      setLoginOpen(false);
      setUserId(nextUserId);
      localStorage.setItem(USER_KEY, String(nextUserId));
      refreshAll();
      notify('success', `已登录用户 ${nextUserId}`);
    },
    [notify, refreshAll],
  );

  const selectSymbol = useCallback((symbol: string) => {
    setSelectedSymbol(symbol);
    localStorage.setItem(SYMBOL_KEY, symbol);
  }, []);

  const loadPairs = useCallback(async (signal: AbortSignal) => {
    try {
      const list = await fetchPairs(signal);
      if (signal.aborted) {
        return;
      }
      setPairs(list);
      setPairsError(null);
      setOnline(true);
      setSelectedSymbol((current) => {
        if (current && list.some((pair) => pair.symbol === current)) {
          return current;
        }
        const next = list[0]?.symbol ?? '';
        if (next) {
          localStorage.setItem(SYMBOL_KEY, next);
        }
        return next;
      });
    } catch (error) {
      if (isAbortError(error) || signal.aborted) {
        return;
      }
      setPairsError(describeError(error));
      setOnline(false);
    }
  }, []);

  const loadCurrencies = useCallback(async (signal: AbortSignal) => {
    try {
      const list = await fetchCurrencies(signal);
      if (!signal.aborted) {
        setCurrencies(list);
      }
    } catch (error) {
      if (!isAbortError(error)) {
        // 币种接口不可用时前端还有 fallback 精度表，不阻塞交易。
        console.warn('currencies unavailable, using fallback scales', error);
      }
    }
  }, []);

  const loadBalances = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade') {
        return;
      }
      try {
        const list = await fetchBalances(userId, signal);
        if (signal.aborted) {
          return;
        }
        setBalances(list);
        setBalancesError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setBalancesError(describeError(error));
      }
    },
    [userId, view],
  );

  const loadKlines = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade' || !selectedSymbol) {
        return;
      }
      try {
        const list = await fetchKlines(selectedSymbol, 180, signal);
        if (signal.aborted) {
          return;
        }
        setKlines(list);
        setMarketError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setMarketError(describeError(error));
      }
    },
    [selectedSymbol, view],
  );

  const loadTrades = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade' || !selectedSymbol) {
        return;
      }
      try {
        const list = await fetchTrades(selectedSymbol, 50, signal);
        if (signal.aborted) {
          return;
        }
        setTrades(list);
        setMarketError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setMarketError(describeError(error));
      }
    },
    [selectedSymbol, view],
  );

  const loadOpenOrders = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade') {
        return;
      }
      try {
        const list = await fetchOpenOrders(userId, 100, signal);
        if (signal.aborted) {
          return;
        }
        setOpenOrders(list);
        setOrdersError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setOrdersError(describeError(error));
      }
    },
    [userId, view],
  );

  const loadFundsFlows = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade') {
        return;
      }
      try {
        const list = await fetchUserFundsFlows(userId, 50, signal);
        if (signal.aborted) {
          return;
        }
        setFundsFlows(list);
        setFundsFlowsError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setFundsFlowsError(describeError(error));
      }
    },
    [userId, view],
  );

  const loadOrderBook = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade' || !selectedSymbol) {
        return;
      }
      try {
        const depth = await fetchOrderBook(selectedSymbol, 10, signal);
        if (signal.aborted) {
          return;
        }
        setOrderBook(depth);
        setOrderBookError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setOrderBookError(describeError(error));
      }
    },
    [selectedSymbol, view],
  );

  const loadLastPrices = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade' || pairs.length === 0) {
        return;
      }
      const results = await Promise.allSettled(
        pairs.map((pair) => fetchKlines(pair.symbol, 1, signal)),
      );
      if (signal.aborted) {
        return;
      }
      const next: Record<string, number> = {};
      results.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          const latest = result.value[result.value.length - 1];
          if (latest) {
            next[pairs[index].symbol] = latest.closePrice;
          }
        }
      });
      setLastPrices((current) => ({ ...current, ...next }));
    },
    [pairs, view],
  );

  usePolling(loadPairs, 30_000, [refreshNonce]);
  usePolling(loadCurrencies, 60_000, [refreshNonce]);

  useEffect(() => {
    setBalances([]);
    setBalancesError(null);
    setOpenOrders([]);
    setOrdersError(null);
    setFundsFlows([]);
    setFundsFlowsError(null);
  }, [userId]);

  useEffect(() => {
    setKlines([]);
    setTrades([]);
    setMarketError(null);
    setOrderBook(null);
    setOrderBookError(null);
  }, [selectedSymbol]);

  usePolling(loadBalances, 5_000, [userId, refreshNonce, balancesNonce, view]);
  usePolling(loadKlines, 5_000, [selectedSymbol, refreshNonce, view]);
  usePolling(loadTrades, 3_000, [selectedSymbol, refreshNonce, view]);
  usePolling(loadOpenOrders, 3_000, [userId, refreshNonce, view]);
  usePolling(loadFundsFlows, 5_000, [userId, refreshNonce, view]);
  usePolling(loadOrderBook, 3_000, [selectedSymbol, refreshNonce, view]);
  usePolling(loadLastPrices, 15_000, [pairs, refreshNonce, view]);

  const pair = useMemo(
    () => pairs.find((item) => item.symbol === selectedSymbol),
    [pairs, selectedSymbol],
  );

  const currencyScales = useMemo(() => {
    const scales: Record<string, number> = {
      BTC: 8,
      ETH: 8,
      USDT: 6,
      USDC: 6,
      USD: 2,
    };
    for (const item of pairs) {
      if (scales[item.baseCurrency] === undefined) {
        scales[item.baseCurrency] = item.quantityScale;
      }
      if (scales[item.quoteCurrency] === undefined) {
        scales[item.quoteCurrency] = 6;
      }
    }
    for (const currency of currencies) {
      scales[currency.code] = currency.scale;
    }
    return scales;
  }, [currencies, pairs]);

  const lastPrice =
    klines.length > 0 ? klines[klines.length - 1].closePrice : lastPrices[selectedSymbol];

  return (
    <div className="app-shell">
      <Header
        userId={userId}
        online={online}
        view={view}
        onNavigate={setView}
        onOpenLogin={() => setLoginOpen(true)}
      />

      {pairsError && (
        <div className="connection-banner">
          <span>
            <b>无法连接后端 API</b> — {pairsError}。请先启动 Spring Boot（默认 8080 端口），
            再点击重试。
          </span>
          <button type="button" onClick={refreshAll}>
            重试
          </button>
        </div>
      )}

      {view === 'trade' ? (
        <main className="main-grid">
          <div className="center-column">
            <ChartPanel
              pair={pair}
              klines={klines}
              error={marketError}
              onOpenMarkets={() => setMarketPickerOpen(true)}
            />
            <AccountActivityPanel
              pairs={pairs}
              orders={openOrders}
              ordersError={ordersError}
              flows={fundsFlows}
              flowsError={fundsFlowsError}
              currencyScales={currencyScales}
            />
          </div>

          <BookTradesPanel
            pair={pair}
            orderBook={orderBook}
            orderBookError={orderBookError}
            trades={trades}
            tradesError={marketError}
          />

          <aside className="right-column">
            <OrderForm
              pair={pair}
              userId={userId}
              balances={balances}
              currencyScales={currencyScales}
              lastPrice={lastPrice}
              bestAsk={orderBook?.bestAsk ?? undefined}
              onOrderPlaced={handleOrderPlaced}
              onRefreshBalances={refreshBalances}
              notify={notify}
            />
            <BalancesPanel
              userId={userId}
              balances={balances}
              pair={pair}
              currencyScales={currencyScales}
              error={balancesError}
              onRefresh={refreshBalances}
            />
          </aside>
        </main>
      ) : (
        <AdminPage
          pairs={pairs}
          currencies={currencies}
          currencyScales={currencyScales}
          online={online}
          notify={notify}
        />
      )}

      <footer className="statusbar">
        <span>
          <i className={`status-dot ${online === false ? 'offline' : 'online'}`} />
          {online === null ? '正在连接 8080…' : online ? 'REST API 已连接' : 'REST API 未连接'}
        </span>
        {view === 'trade' ? (
          <>
            <span>行情：QuestDB md_kline / md_trade</span>
            <span>价格与数量均按交易对精度换算</span>
          </>
        ) : (
          <>
            <span>后台管理：QuestDB funds_flow / md_kline</span>
            <span>删除操作使用 TRUNCATE，不可恢复</span>
          </>
        )}
      </footer>

      <ToastStack toasts={toasts} onDismiss={dismissToast} />

      <MarketPickerModal
        open={marketPickerOpen}
        pairs={pairs}
        selectedSymbol={selectedSymbol}
        lastPrices={lastPrices}
        onSelect={selectSymbol}
        onClose={() => setMarketPickerOpen(false)}
      />

      <RechargeModal
        open={loginOpen}
        currentUserId={userId}
        currencies={currencies}
        currencyScales={currencyScales}
        defaultCurrency={pair?.quoteCurrency ?? 'USDT'}
        onClose={() => setLoginOpen(false)}
        onRechargeSuccess={handleRechargeSuccess}
        onLogin={handleLogin}
      />
    </div>
  );
}

export default App;
