import { useEffect, useRef, useState } from 'react';
import type { DepthLevel, Kline, OrderBookDepth, Trade } from '../api';

export type OrderBookSocketStatus = 'connecting' | 'open' | 'closed';

export interface OrderBookSocketState {
  orderBook: OrderBookDepth | null;
  /** 盘口帧附带的最新成交（最新在前），与订单簿共用同一次订阅与推送。 */
  trades: Trade[];
  /**
   * 盘口帧附带的各周期最新 K 线，每个配置周期一根（只含当前正在聚合的柱子）。
   * 前端按 interval 匹配当前周期后，与最后一根柱子比较时间戳决定覆盖或追加。
   */
  latestKlines: Kline[];
  error: string | null;
  status: OrderBookSocketStatus;
}

interface OrderBookSocketMessage {
  type?: string;
  symbol?: string | null;
  depth?: number | null;
  bids?: DepthLevel[];
  asks?: DepthLevel[];
  bestBid?: number | null;
  bestAsk?: number | null;
  trades?: Trade[];
  klines?: Kline[];
  timestamp?: string;
  reason?: string | null;
}

const RECONNECT_MIN_MS = 1_000;
const RECONNECT_MAX_MS = 8_000;
const DEFAULT_DEPTH = 10;

function orderBookSocketUrl(): string {
  const explicit = import.meta.env.VITE_ORDERBOOK_WS_URL as string | undefined;
  if (explicit) {
    return explicit;
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws/orderbook`;
}

/**
 * 订阅撮合引擎盘口 WebSocket。
 *
 * <p>连接建立后发送 `subscribe`；切换交易对时用最新 symbol 重新订阅，服务端以新订阅
 * 替换旧订阅。收到旧交易对的推送会被丢弃，避免切换瞬间串台。每帧同时更新订单簿、
 * 最新成交与各周期最新 K 线，三者共用同一次订阅。连接异常时按 1s → 8s 指数退避自动
 * 重连，直到组件卸载或 `enabled` 变为 false。
 */
export function useOrderBookSocket(
  symbol: string,
  depth: number = DEFAULT_DEPTH,
  enabled = true,
): OrderBookSocketState {
  const [orderBook, setOrderBook] = useState<OrderBookDepth | null>(null);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [latestKlines, setLatestKlines] = useState<Kline[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<OrderBookSocketStatus>('closed');

  const symbolRef = useRef(symbol);
  const depthRef = useRef(depth);
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<number | null>(null);
  const reconnectAttempts = useRef(0);

  // 消息回调里始终读取最新订阅；切换交易对后不会用旧闭包订阅。
  symbolRef.current = symbol;
  depthRef.current = depth;

  useEffect(() => {
    if (!enabled) {
      setStatus('closed');
      return;
    }

    setStatus('connecting');
    let disposed = false;

    const clearReconnectTimer = () => {
      if (reconnectTimer.current !== null) {
        window.clearTimeout(reconnectTimer.current);
        reconnectTimer.current = null;
      }
    };

    const subscribe = (socket: WebSocket) => {
      if (socket.readyState !== WebSocket.OPEN || !symbolRef.current) {
        return;
      }
      socket.send(
        JSON.stringify({
          action: 'subscribe',
          symbol: symbolRef.current,
          depth: depthRef.current,
        }),
      );
    };

    const scheduleReconnect = () => {
      if (disposed) {
        return;
      }
      clearReconnectTimer();
      const delay = Math.min(
        RECONNECT_MIN_MS * 2 ** reconnectAttempts.current,
        RECONNECT_MAX_MS,
      );
      reconnectAttempts.current += 1;
      reconnectTimer.current = window.setTimeout(connect, delay);
    };

    const connect = () => {
      if (disposed) {
        return;
      }
      setStatus('connecting');

      let socket: WebSocket;
      try {
        socket = new WebSocket(orderBookSocketUrl());
      } catch {
        setStatus('closed');
        scheduleReconnect();
        return;
      }
      socketRef.current = socket;

      socket.onopen = () => {
        if (disposed) {
          socket.close();
          return;
        }
        reconnectAttempts.current = 0;
        setStatus('open');
        setError(null);
        subscribe(socket);
      };

      socket.onmessage = (event) => {
        let message: OrderBookSocketMessage;
        try {
          message = JSON.parse(String(event.data)) as OrderBookSocketMessage;
        } catch {
          return;
        }
        if (!message || typeof message.type !== 'string') {
          return;
        }

        if (message.type === 'orderbook') {
          // 切换交易对后，服务端可能还会推来一帧旧交易对的数据。
          if (!message.symbol || message.symbol !== symbolRef.current) {
            return;
          }
          setOrderBook({
            symbol: message.symbol,
            depth: message.depth ?? depthRef.current,
            bids: message.bids ?? [],
            asks: message.asks ?? [],
            bestBid: message.bestBid ?? null,
            bestAsk: message.bestAsk ?? null,
            timestamp: message.timestamp ?? new Date().toISOString(),
          });
          setTrades(message.trades ?? []);
          setLatestKlines(message.klines ?? []);
          setError(null);
          return;
        }

        if (message.type === 'error') {
          if (!message.symbol || message.symbol === symbolRef.current) {
            setError(message.reason ?? 'ORDER_BOOK_STREAM_ERROR');
          }
        }
      };

      socket.onerror = () => {
        // 具体的重连由 onclose 统一处理。
      };

      socket.onclose = () => {
        if (socketRef.current === socket) {
          socketRef.current = null;
        }
        if (disposed) {
          return;
        }
        setStatus('closed');
        scheduleReconnect();
      };
    };

    // 延后一个事件循环再建连：React StrictMode 开发模式会“挂载→卸载→挂载”，
    // 这样第一次挂载的定时器会先被清理，不会创建又立刻关闭一个 socket。
    reconnectTimer.current = window.setTimeout(connect, 0);

    return () => {
      disposed = true;
      clearReconnectTimer();
      const socket = socketRef.current;
      socketRef.current = null;
      if (socket) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
      }
      setStatus('closed');
    };
  }, [enabled]);

  useEffect(() => {
    setOrderBook(null);
    setTrades([]);
    setLatestKlines([]);
    setError(null);
    if (!enabled) {
      return;
    }
    const socket = socketRef.current;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ action: 'subscribe', symbol, depth }));
    }
  }, [symbol, depth, enabled]);

  return { orderBook, trades, latestKlines, error, status };
}
