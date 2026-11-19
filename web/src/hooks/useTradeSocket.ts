import { useCallback, useEffect, useRef, useState } from 'react';
import {
  ApiError,
  cancelOrder as restCancelOrder,
  placeOrder as restPlaceOrder,
  type CancelOrderRequest,
  type CancelOrderResponse,
  type PlaceOrderRequest,
  type PlaceOrderResponse,
} from '../api';

export type TradeSocketStatus = 'connecting' | 'open' | 'closed';

interface TradeAckMessage {
  type?: string;
  stage?: string | null;
  requestId?: string | null;
  success?: boolean | null;
  reason?: string | null;
  orderId?: number | null;
  symbol?: string | null;
  userId?: number | null;
}

interface PendingRequest {
  resolve: (value: PlaceOrderResponse | CancelOrderResponse) => void;
  reject: (reason: unknown) => void;
  timer: number;
}

export interface TradeSocketState {
  status: TradeSocketStatus;
  /** 已建立连接并收到 loggedIn 回执，后续下单/撤单走 WebSocket。 */
  ready: boolean;
  /**
   * 下单：WS 未就绪时自动回退 REST；已经通过 WS 发出后不再回退，避免重复下单。
   * 返回结构与 REST 的 PlaceOrderResponse 一致。
   */
  placeOrder: (request: PlaceOrderRequest) => Promise<PlaceOrderResponse>;
  /** 撤单：规则同上。 */
  cancelOrder: (request: CancelOrderRequest) => Promise<CancelOrderResponse>;
}

const RECONNECT_MIN_MS = 1_000;
const RECONNECT_MAX_MS = 8_000;
const ACK_TIMEOUT_MS = 5_000;

function tradeSocketUrl(): string {
  const explicit = import.meta.env.VITE_TRADE_WS_URL as string | undefined;
  if (explicit) {
    return explicit;
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws/trade`;
}

/**
 * 用户级交易 WebSocket：连接 /ws/trade，首条消息 login，下单/撤单按 requestId 等待 ingress ack。
 *
 * <p>ack 只代表“命令进入 ingress ring”，不代表委托已受理 / 冻结 / 成交；超时后应通过
 * requestId 查单，不能自动重试（后端不做 requestId 幂等）。
 *
 * <p>WS 未连接或尚未收到 loggedIn 时，placeOrder/cancelOrder 自动回退到现有 REST 接口；
 * 一旦请求已通过 WS 发出，就不会再回退 REST，避免同一 requestId 产生两笔委托。
 */
export function useTradeSocket(userId: number): TradeSocketState {
  const [status, setStatus] = useState<TradeSocketStatus>('closed');
  const [ready, setReady] = useState(false);

  const socketRef = useRef<WebSocket | null>(null);
  const readyRef = useRef(false);
  const pendingRef = useRef(new Map<string, PendingRequest>());
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const userIdRef = useRef(userId);
  userIdRef.current = userId;

  const clearPending = useCallback((reason: string) => {
    for (const pending of pendingRef.current.values()) {
      window.clearTimeout(pending.timer);
      pending.reject(new ApiError(reason, 0));
    }
    pendingRef.current.clear();
  }, []);

  useEffect(() => {
    if (userId <= 0) {
      setStatus('closed');
      setReady(false);
      readyRef.current = false;
      clearPending('TRADE_SOCKET_CLOSED');
      return;
    }

    let disposed = false;
    setStatus('connecting');
    setReady(false);
    readyRef.current = false;

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const scheduleReconnect = () => {
      if (disposed) {
        return;
      }
      clearReconnectTimer();
      const delay = Math.min(
        RECONNECT_MIN_MS * 2 ** reconnectAttemptsRef.current,
        RECONNECT_MAX_MS,
      );
      reconnectAttemptsRef.current += 1;
      reconnectTimerRef.current = window.setTimeout(connect, delay);
    };

    const rejectPending = (requestId: string, reason: string) => {
      const pending = pendingRef.current.get(requestId);
      if (!pending) {
        return;
      }
      window.clearTimeout(pending.timer);
      pendingRef.current.delete(requestId);
      pending.reject(new ApiError(reason, 0));
    };

    const resolvePending = (message: TradeAckMessage) => {
      const requestId = message.requestId;
      if (!requestId) {
        return;
      }
      const pending = pendingRef.current.get(requestId);
      if (!pending) {
        return;
      }
      window.clearTimeout(pending.timer);
      pendingRef.current.delete(requestId);
      pending.resolve({
        success: message.success === true,
        reason: message.reason ?? null,
        requestId,
        orderId: message.orderId ?? null,
        symbol: message.symbol ?? null,
      });
    };

    const handleMessage = (event: MessageEvent) => {
      let message: TradeAckMessage;
      try {
        message = JSON.parse(String(event.data)) as TradeAckMessage;
      } catch {
        return;
      }
      if (!message || typeof message.type !== 'string') {
        return;
      }
      switch (message.type) {
        case 'loggedIn':
          readyRef.current = true;
          setReady(true);
          return;
        case 'orderAck':
          resolvePending(message);
          return;
        case 'orderAckTimeout': {
          if (message.requestId) {
            rejectPending(message.requestId, 'ACK_TIMEOUT');
          }
          return;
        }
        case 'error':
          if (message.requestId) {
            rejectPending(message.requestId, message.reason ?? 'TRADE_REQUEST_FAILED');
          }
          return;
        case 'pong':
        default:
          return;
      }
    };

    const connect = () => {
      if (disposed) {
        return;
      }
      setStatus('connecting');

      let socket: WebSocket;
      try {
        socket = new WebSocket(tradeSocketUrl());
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
        reconnectAttemptsRef.current = 0;
        setStatus('open');
        socket.send(JSON.stringify({ action: 'login', userId: userIdRef.current }));
      };

      socket.onmessage = handleMessage;

      socket.onerror = () => {
        // 具体重连由 onclose 统一处理。
      };

      socket.onclose = () => {
        if (socketRef.current === socket) {
          socketRef.current = null;
        }
        readyRef.current = false;
        setReady(false);
        if (disposed) {
          return;
        }
        setStatus('closed');
        clearPending('TRADE_SOCKET_CLOSED');
        scheduleReconnect();
      };
    };

    // 与盘口 hook 相同：延后一个事件循环，避免 React StrictMode 首次挂载立刻建连又关闭。
    reconnectTimerRef.current = window.setTimeout(connect, 0);

    return () => {
      disposed = true;
      clearReconnectTimer();
      clearPending('TRADE_SOCKET_CLOSED');
      const socket = socketRef.current;
      socketRef.current = null;
      readyRef.current = false;
      if (socket) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
      }
      setReady(false);
      setStatus('closed');
    };
  }, [userId, clearPending]);

  const sendViaSocket = useCallback(
    (
      action: 'placeOrder' | 'cancelOrder',
      request: PlaceOrderRequest | CancelOrderRequest,
      signal?: AbortSignal,
    ): Promise<PlaceOrderResponse | CancelOrderResponse> => {
      if (signal?.aborted) {
        return Promise.reject(new DOMException('Aborted', 'AbortError'));
      }
      const socket = socketRef.current;
      if (!readyRef.current || !socket || socket.readyState !== WebSocket.OPEN) {
        return Promise.reject(new ApiError('TRADE_SOCKET_NOT_READY', 0));
      }
      const requestId = request.requestId;
      if (!requestId) {
        return Promise.reject(new ApiError('REQUEST_ID_REQUIRED', 0));
      }
      if (pendingRef.current.has(requestId)) {
        return Promise.reject(new ApiError('REQUEST_ID_IN_PROGRESS', 0));
      }

      return new Promise((resolve, reject) => {
        const timer = window.setTimeout(() => {
          pendingRef.current.delete(requestId);
          reject(new ApiError('ACK_TIMEOUT', 0));
        }, ACK_TIMEOUT_MS);
        pendingRef.current.set(requestId, { resolve, reject, timer });
        try {
          socket.send(JSON.stringify({ action, ...request }));
        } catch {
          window.clearTimeout(timer);
          pendingRef.current.delete(requestId);
          reject(new ApiError('TRADE_SOCKET_SEND_FAILED', 0));
        }
      });
    },
    [],
  );

  const placeOrder = useCallback(
    async (request: PlaceOrderRequest): Promise<PlaceOrderResponse> => {
      if (!readyRef.current) {
        return restPlaceOrder(request);
      }
      return sendViaSocket('placeOrder', request) as Promise<PlaceOrderResponse>;
    },
    [sendViaSocket],
  );

  const cancelOrder = useCallback(
    async (request: CancelOrderRequest): Promise<CancelOrderResponse> => {
      if (!readyRef.current) {
        return restCancelOrder(request);
      }
      return sendViaSocket('cancelOrder', request) as Promise<CancelOrderResponse>;
    },
    [sendViaSocket],
  );

  return { status, ready, placeOrder, cancelOrder };
}
