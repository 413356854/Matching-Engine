import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ApiError,
  KLINE_INTERVALS,
  disableBotTrading,
  enableBotTrading,
  fetchBalances,
  fetchBotStatus,
  fetchCurrencies,
  fetchKlines,
  fetchOpenOrders,
  fetchOrderHistory,
  fetchPairs,
  fetchUserFundsFlows,
  fetchUserTrades,
  isAbortError,
  type AccountBalance,
  type BotStatus,
  type Currency,
  type FundsFlow,
  type Kline,
  type KlineInterval,
  type OrderHistory,
  type OrderView,
  type TradingPair,
  type UserTrade,
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
import { describeError, describeReason } from './errors';
import { useOrderBookSocket } from './hooks/useOrderBookSocket';
import { usePolling } from './hooks/usePolling';

const USER_KEY = 'me.userId';
const SYMBOL_KEY = 'me.symbol';
const INTERVAL_KEY = 'me.klineInterval';
const DEFAULT_USER_ID = 1001;
const DEFAULT_KLINE_INTERVAL: KlineInterval = '1m';

function storedUserId(): number {
  const value = Number(localStorage.getItem(USER_KEY));
  return Number.isInteger(value) && value > 0 ? value : DEFAULT_USER_ID;
}

function storedKlineInterval(): KlineInterval {
  const value = localStorage.getItem(INTERVAL_KEY);
  return KLINE_INTERVALS.includes(value as KlineInterval)
    ? (value as KlineInterval)
    : DEFAULT_KLINE_INTERVAL;
}

/** 两根 K 线的实时字段是否一致；避免 500ms 推送在数据没变时触发图表重绘。 */
function sameKline(left: Kline, right: Kline): boolean {
  return (
    left.openPrice === right.openPrice &&
    left.highPrice === right.highPrice &&
    left.lowPrice === right.lowPrice &&
    left.closePrice === right.closePrice &&
    left.volume === right.volume &&
    left.amount === right.amount &&
    left.tradeCount === right.tradeCount
  );
}

/**
 * 把 WebSocket 推送的最新 K 线合并进面板数据：
 * 时间戳相同说明还是同一根柱子，覆盖最后一根；时间戳更大说明进入新周期，追加新柱子。
 * 更早的推送（切换交易对 / 周期瞬间的旧帧）直接忽略。
 */
function mergePushedKline(
  current: Kline[],
  pushed: Kline[],
  interval: KlineInterval,
): Kline[] {
  const latest = pushed.find((kline) => kline.interval === interval);
  if (!latest) {
    return current;
  }
  const last = current[current.length - 1];
  if (!last) {
    return [latest];
  }
  const lastTime = Date.parse(last.bucketStart);
  const latestTime = Date.parse(latest.bucketStart);
  if (latestTime === lastTime) {
    return sameKline(last, latest) ? current : [...current.slice(0, -1), latest];
  }
  return latestTime > lastTime ? [...current, latest] : current;
}

/**
 * REST 查询读的是 QuestDB，最多比内存推送落后一个落库间隔（默认 1s）；合并时保留更新的一份，
 * 避免 5 秒轮询把 WebSocket 已经推上来的实时柱子回退。
 */
function mergeStoredKlines(stored: Kline[], current: Kline[]): Kline[] {
  const storedLast = stored[stored.length - 1];
  const currentLast = current[current.length - 1];
  if (!storedLast) {
    // 库里没有历史（新交易对或管理端刚清空）：以服务器结果为准，后续 WS 推送会补最新柱。
    return stored;
  }
  if (!currentLast) {
    return stored;
  }
  const storedTime = Date.parse(storedLast.bucketStart);
  const currentTime = Date.parse(currentLast.bucketStart);
  if (currentTime < storedTime) {
    return stored;
  }
  if (currentTime === storedTime) {
    // 同一根柱子的成交笔数只增不减，笔数更多的一份就是更新的内存快照。
    return currentLast.tradeCount > storedLast.tradeCount
      ? [...stored.slice(0, -1), currentLast]
      : stored;
  }
  // 内存已经推进到更晚的 bucket：服务器结果作为历史，末尾接上实时柱子。
  return [...stored, ...current.filter((kline) => Date.parse(kline.bucketStart) > storedTime)];
}

/**
 * 交易对列表内容是否一致。轮询拿到相同配置时保留旧数组引用，
 * 避免依赖 pairs 的 effect（如最近价查询）被无意义地重复触发。
 */
function samePairs(left: TradingPair[], right: TradingPair[]): boolean {
  if (left === right) {
    return true;
  }
  if (left.length !== right.length) {
    return false;
  }
  return left.every((pair, index) => {
    const other = right[index];
    return (
      pair.id === other.id &&
      pair.symbol === other.symbol &&
      pair.baseCurrency === other.baseCurrency &&
      pair.quoteCurrency === other.quoteCurrency &&
      pair.priceScale === other.priceScale &&
      pair.quantityScale === other.quantityScale &&
      pair.minOrderQuantity === other.minOrderQuantity &&
      pair.minOrderAmount === other.minOrderAmount &&
      pair.makerFeeBps === other.makerFeeBps &&
      pair.takerFeeBps === other.takerFeeBps &&
      pair.status === other.status &&
      pair.sortOrder === other.sortOrder
    );
  });
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
  const [klineInterval, setKlineInterval] = useState<KlineInterval>(storedKlineInterval);
  const [openOrders, setOpenOrders] = useState<OrderView[]>([]);
  const [orderHistory, setOrderHistory] = useState<OrderHistory[]>([]);
  const [userTrades, setUserTrades] = useState<UserTrade[]>([]);
  const [fundsFlows, setFundsFlows] = useState<FundsFlow[]>([]);
  const [lastPrices, setLastPrices] = useState<Record<string, number>>({});
  const [online, setOnline] = useState<boolean | null>(null);
  const [pairsError, setPairsError] = useState<string | null>(null);
  const [balancesError, setBalancesError] = useState<string | null>(null);
  const [marketError, setMarketError] = useState<string | null>(null);
  const [ordersError, setOrdersError] = useState<string | null>(null);
  const [orderHistoryError, setOrderHistoryError] = useState<string | null>(null);
  const [userTradesError, setUserTradesError] = useState<string | null>(null);
  const [fundsFlowsError, setFundsFlowsError] = useState<string | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [balancesNonce, setBalancesNonce] = useState(0);
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const [loginOpen, setLoginOpen] = useState(false);
  const [marketPickerOpen, setMarketPickerOpen] = useState(false);
  const [view, setView] = useState<AppView>('trade');
  const [botStatus, setBotStatus] = useState<BotStatus | null>(null);
  const [botBusy, setBotBusy] = useState(false);

  const toastTimers = useRef(new Map<number, number>());
  const refreshTimer = useRef<number | null>(null);
  // 首次 REST K 线返回前不接收 WebSocket 增量，避免“只有一根柱子”的瞬间触发图表自适应缩放。
  const klinesReadyRef = useRef(false);

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

  const loadBotStatus = useCallback(async (signal: AbortSignal) => {
    try {
      const status = await fetchBotStatus(signal);
      if (!signal.aborted) {
        setBotStatus(status);
      }
    } catch {
      // 机器人接口不可用时保持上一次状态；初始失败则不显示按钮，不影响其它轮询。
    }
  }, []);

  const handleToggleBot = useCallback(async () => {
    if (botBusy) {
      return;
    }
    const enabling = !botStatus?.trading;
    setBotBusy(true);
    try {
      const status = enabling ? await enableBotTrading() : await disableBotTrading();
      setBotStatus(status);
      if (!enabling) {
        notify('info', '自动交易已关闭');
      } else if (status.binanceConnected) {
        notify('success', '自动交易已开启，币安行情已连接');
      } else {
        notify('success', '自动交易已开启，正在连接币安行情…');
      }
    } catch (error) {
      if (error instanceof ApiError && error.reason === 'BINANCE_UNREACHABLE') {
        notify('error', '开启需要配置科学联网币安（币安测试接口不可达）');
      } else {
        notify('error', describeError(error));
      }
    } finally {
      setBotBusy(false);
    }
  }, [botBusy, botStatus?.trading, notify]);

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
      setPairs((current) => (samePairs(current, list) ? current : list));
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
        const list = await fetchKlines(selectedSymbol, 180, klineInterval, signal);
        if (signal.aborted) {
          return;
        }
        klinesReadyRef.current = true;
        // 查询结果可能落后于 WebSocket 实时推送，合并时保留更新的一根柱子。
        setKlines((current) => mergeStoredKlines(list, current));
        setMarketError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setMarketError(describeError(error));
      }
    },
    [klineInterval, selectedSymbol, view],
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

  const loadOrderHistory = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade') {
        return;
      }
      try {
        const list = await fetchOrderHistory(userId, undefined, 50, signal);
        if (signal.aborted) {
          return;
        }
        setOrderHistory(list);
        setOrderHistoryError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setOrderHistoryError(describeError(error));
      }
    },
    [userId, view],
  );

  const loadUserTrades = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade') {
        return;
      }
      try {
        const list = await fetchUserTrades(userId, undefined, 50, signal);
        if (signal.aborted) {
          return;
        }
        setUserTrades(list);
        setUserTradesError(null);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) {
          return;
        }
        setUserTradesError(describeError(error));
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

  // 用户记录面板的手动刷新：当前委托与三个历史列表并发拉取，按钮的加载动画跟随 Promise 完成。
  const refreshActivity = useCallback(async () => {
    const controller = new AbortController();
    try {
      await Promise.all([
        loadOpenOrders(controller.signal),
        loadOrderHistory(controller.signal),
        loadUserTrades(controller.signal),
        loadFundsFlows(controller.signal),
      ]);
    } finally {
      controller.abort();
    }
  }, [loadFundsFlows, loadOpenOrders, loadOrderHistory, loadUserTrades]);

  const loadLastPrices = useCallback(
    async (signal: AbortSignal) => {
      if (view !== 'trade' || pairs.length === 0) {
        return;
      }
      const results = await Promise.allSettled(
        pairs.map((pair) => fetchKlines(pair.symbol, 1, '1m', signal)),
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
  // 机器人状态只在页面加载时查询一次；开关操作会用接口返回的最新状态覆盖。
  usePolling(loadBotStatus, null, []);

  useEffect(() => {
    setBalances([]);
    setBalancesError(null);
    setOpenOrders([]);
    setOrdersError(null);
    setOrderHistory([]);
    setOrderHistoryError(null);
    setUserTrades([]);
    setUserTradesError(null);
    setFundsFlows([]);
    setFundsFlowsError(null);
  }, [userId]);

  useEffect(() => {
    klinesReadyRef.current = false;
    setKlines([]);
    setMarketError(null);
  }, [selectedSymbol, klineInterval]);

  const handleKlineIntervalChange = useCallback((interval: KlineInterval) => {
    localStorage.setItem(INTERVAL_KEY, interval);
    setKlineInterval(interval);
  }, []);

  // 余额不再自动轮询，由「刷新余额」按钮以及下单 / 撤单 / 登录等事件触发加载。
  usePolling(loadBalances, null, [userId, refreshNonce, balancesNonce, view]);
  // K 线已接入 WebSocket 实时推送，只在首次加载 / 切换交易对与周期时查询一次历史数据。
  usePolling(loadKlines, null, [selectedSymbol, klineInterval, refreshNonce, view]);
  // 用户记录（当前委托 / 历史委托 / 历史成交 / 历史流水）不再自动轮询，由面板上的刷新按钮手动触发。
  usePolling(loadOpenOrders, null, [userId, refreshNonce, view]);
  usePolling(loadOrderHistory, null, [userId, refreshNonce, view]);
  usePolling(loadUserTrades, null, [userId, refreshNonce, view]);
  usePolling(loadFundsFlows, null, [userId, refreshNonce, view]);
  // 非当前交易对的最近价只供行情选择弹窗展示：弹窗打开时查询一次，不做定时轮询。
  usePolling(loadLastPrices, null, [pairs, refreshNonce, view, marketPickerOpen]);

  // 盘口、最新成交与最新 K 线共用同一帧 WebSocket 推送（500ms）；切换交易对时自动重新订阅。
  const {
    orderBook,
    trades,
    latestKlines,
    error: orderBookError,
    status: orderBookStream,
  } = useOrderBookSocket(selectedSymbol, 10, view === 'trade' && selectedSymbol.length > 0);

  // K 线随盘口帧一起推送：同一时间戳覆盖最后一根，出现更晚的 bucket 就追加新柱子。
  useEffect(() => {
    if (!klinesReadyRef.current || latestKlines.length === 0) {
      return;
    }
    setKlines((current) => mergePushedKline(current, latestKlines, klineInterval));
  }, [klineInterval, latestKlines]);

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
              interval={klineInterval}
              onIntervalChange={handleKlineIntervalChange}
              error={marketError}
              onOpenMarkets={() => setMarketPickerOpen(true)}
              botStatus={botStatus}
              botBusy={botBusy}
              onToggleBot={handleToggleBot}
            />
            <AccountActivityPanel
              pairs={pairs}
              orders={openOrders}
              ordersError={ordersError}
              history={orderHistory}
              historyError={orderHistoryError}
              trades={userTrades}
              tradesError={userTradesError}
              flows={fundsFlows}
              flowsError={fundsFlowsError}
              currencyScales={currencyScales}
              userId={userId}
              // 撤单与下单一样异步生效：先刷新一次，1.5s 后再刷新，等待解冻与状态落定。
              onOrderCanceled={handleOrderPlaced}
              onRefreshActivity={refreshActivity}
              notify={notify}
            />
          </div>

          <BookTradesPanel
            pair={pair}
            orderBook={orderBook}
            orderBookError={orderBookError ? describeReason(orderBookError) : null}
            orderBookStream={orderBookStream}
            trades={trades}
            tradesError={orderBookError ? describeReason(orderBookError) : null}
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
            <span>
              <i
                className={`status-dot ${
                  orderBookStream === 'open'
                    ? 'online'
                    : orderBookStream === 'connecting'
                      ? 'pending'
                      : 'offline'
                }`}
              />
              {orderBookStream === 'open'
                ? '盘口 WebSocket 已连接'
                : orderBookStream === 'connecting'
                  ? '盘口 WebSocket 连接中'
                  : '盘口 WebSocket 未连接'}
            </span>
            <span>行情：QuestDB md_kline + 内存成交缓存</span>
            <span>价格与数量均按交易对精度换算</span>
          </>
        ) : (
          <>
            <span>后台管理：QuestDB funds_flow / ord_history / user_trade / md_kline · MySQL snapshot_backup</span>
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
