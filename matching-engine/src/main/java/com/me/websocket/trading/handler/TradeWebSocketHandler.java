package com.me.websocket.trading.handler;

import com.me.aeron.AeronProperties;
import com.me.shared.gateway.AeronAction;
import com.me.shared.gateway.AeronOrderRequest;
import com.me.shared.order.OrderRequestLimits;
import com.me.websocket.trading.config.TradingProperties;
import com.me.websocket.trading.dto.TradeClientMessage;
import com.me.websocket.trading.service.TradeAeronPublisher;
import com.me.websocket.trading.service.TradeBinaryCodec;
import com.me.websocket.trading.service.TradeMessageCodec;
import com.me.websocket.trading.service.TradeSession;
import com.me.websocket.trading.service.TradeSessionRegistry;
import com.me.websocket.trading.service.TradeSessionSender;
import com.me.websocket.trading.vo.TradeServerMessage;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

/**
 * {@code /ws/trade} 的消息处理：login、placeOrder、cancelOrder。
 *
 * <p>WS 层只做连接级校验（登录态、requestId、限流、消息大小）和 Aeron offer；字段级业务
 * 校验、委托号生成、ring 入队由币币模块的 {@code AeronRequestProcessor} 完成。成功时本类
 * 不直接回 ack，而是等待币币模块通过 Aeron 返回的 ingress ack，由
 * {@code TradeAckPoller} 路由回当前连接。
 *
 * <p>同一端点支持两种协议：文本帧为 JSON（兼容现有前端）；二进制帧为 SBE（见
 * {@link TradeBinaryCodec}）。连接收到首个二进制帧后固定为二进制模式，回执全部使用
 * 二进制帧；文本连接保持 JSON。两条路径共用同一套校验、限流和 Aeron offer 逻辑。
 *
 * <p>异常 / ring 满 / offer 失败一律丢弃订单并回失败信息，不重试；客户端收到
 * {@code orderAckTimeout} 后应查单，不能自动重发。
 */
public final class TradeWebSocketHandler extends AbstractWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(TradeWebSocketHandler.class);

    private static final String ACTION_LOGIN = "login";
    private static final String ACTION_PLACE = "placeorder";
    private static final String ACTION_CANCEL = "cancelorder";
    private static final String ACTION_PING = "ping";

    private final TradingProperties properties;
    private final AeronProperties aeronProperties;
    private final TradeMessageCodec codec;
    private final TradeBinaryCodec binaryCodec;
    private final TradeSessionRegistry registry;
    private final TradeSessionSender sender;
    private final TradeAeronPublisher publisher;

    public TradeWebSocketHandler(TradingProperties properties, AeronProperties aeronProperties,
                                 TradeMessageCodec codec, TradeBinaryCodec binaryCodec,
                                 TradeSessionRegistry registry,
                                 TradeSessionSender sender, TradeAeronPublisher publisher) {
        this.properties = properties;
        this.aeronProperties = aeronProperties;
        this.codec = codec;
        this.binaryCodec = binaryCodec;
        this.registry = registry;
        this.sender = sender;
        this.publisher = publisher;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        TradeSession state = registry.register(session);
        log.debug("trade websocket connected: connectionId={}", state.connectionId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        if (message.getPayloadLength() > properties.maxMessageBytes()) {
            sendError(session, null, "MESSAGE_TOO_LARGE");
            return;
        }

        TradeClientMessage command;
        try {
            command = codec.read(message.getPayload());
        } catch (RuntimeException e) {
            sendError(session, null, "MESSAGE_INVALID");
            return;
        }
        if (command == null) {
            sendError(session, null, "MESSAGE_INVALID");
            return;
        }

        try {
            dispatch(session, command);
        } catch (RuntimeException e) {
            log.error("trade websocket message failed: connectionId={}",
                    TradeSessionRegistry.connectionId(session), e);
            sendError(session, command.requestId(), "INTERNAL_ERROR");
        }
    }

    /**
     * SBE 二进制帧入口：首个二进制帧把连接切到二进制模式，之后所有回执都是二进制帧。
     * 解码失败也按二进制模式回 {@code Error}，避免同一连接混用文本 / 二进制协议。
     */
    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        TradeSession state = registry.get(connectionId);
        if (state != null) {
            state.markBinary();
        }
        if (message.getPayloadLength() > properties.maxMessageBytes()) {
            sendError(session, null, "MESSAGE_TOO_LARGE");
            return;
        }

        TradeClientMessage command;
        try {
            command = binaryCodec.read(message.getPayload());
        } catch (RuntimeException e) {
            sendError(session, null, "MESSAGE_INVALID");
            return;
        }

        try {
            dispatch(session, command);
        } catch (RuntimeException e) {
            log.error("trade websocket binary message failed: connectionId={}",
                    connectionId, e);
            sendError(session, command.requestId(), "INTERNAL_ERROR");
        }
    }

    private void dispatch(WebSocketSession session, TradeClientMessage command) {
        String action = command.action() == null
                ? ""
                : command.action().strip().toLowerCase(Locale.ROOT);
        switch (action) {
            case ACTION_LOGIN -> handleLogin(session, command);
            case ACTION_PLACE -> handlePlace(session, command);
            case ACTION_CANCEL -> handleCancel(session, command);
            case ACTION_PING -> send(session, TradeServerMessage.pong());
            default -> sendError(session, command.requestId(), "ACTION_INVALID");
        }
    }

    private void handleLogin(WebSocketSession session, TradeClientMessage command) {
        Long userId = command.userId();
        if (userId == null) {
            sendError(session, null, "USER_ID_REQUIRED");
            return;
        }
        if (userId <= 0) {
            sendError(session, null, "USER_ID_INVALID");
            return;
        }
        TradeSession state = registry.get(TradeSessionRegistry.connectionId(session));
        if (state == null) {
            sendError(session, null, "CONNECTION_NOT_REGISTERED");
            return;
        }
        if (!state.login(userId)) {
            sendError(session, null, "ALREADY_AUTHENTICATED");
            return;
        }
        send(session, TradeServerMessage.loggedIn(userId));
    }

    private void handlePlace(WebSocketSession session, TradeClientMessage command) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        String requestId = normalizeRequestId(command.requestId());
        TradeSession state = requireLogin(session, connectionId, requestId);
        if (state == null) {
            return;
        }
        if (requestId == null) {
            sendError(session, null, "REQUEST_ID_REQUIRED");
            return;
        }
        if (requestId.length() > OrderRequestLimits.MAX_REQUEST_ID_LENGTH) {
            sendError(session, requestId, "REQUEST_ID_INVALID");
            return;
        }
        if (!state.tryAcquireRateLimit()) {
            sendError(session, requestId, "RATE_LIMITED");
            return;
        }
        if (!publisher.isConnected()) {
            sendError(session, requestId, "TRANSPORT_UNAVAILABLE");
            return;
        }

        AeronOrderRequest request = new AeronOrderRequest(
                AeronOrderRequest.CURRENT_VERSION,
                AeronAction.PLACE,
                requestId,
                connectionId,
                state.userId(),
                replyStreamId(connectionId),
                command.symbol(),
                command.side(),
                command.type(),
                command.price(),
                command.quantity(),
                command.amount(),
                null);
        offer(session, state, requestId, request);
    }

    private void handleCancel(WebSocketSession session, TradeClientMessage command) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        String requestId = normalizeRequestId(command.requestId());
        TradeSession state = requireLogin(session, connectionId, requestId);
        if (state == null) {
            return;
        }
        if (requestId == null) {
            sendError(session, null, "REQUEST_ID_REQUIRED");
            return;
        }
        if (requestId.length() > OrderRequestLimits.MAX_REQUEST_ID_LENGTH) {
            sendError(session, requestId, "REQUEST_ID_INVALID");
            return;
        }
        if (!state.tryAcquireRateLimit()) {
            sendError(session, requestId, "RATE_LIMITED");
            return;
        }
        if (!publisher.isConnected()) {
            sendError(session, requestId, "TRANSPORT_UNAVAILABLE");
            return;
        }

        AeronOrderRequest request = new AeronOrderRequest(
                AeronOrderRequest.CURRENT_VERSION,
                AeronAction.CANCEL,
                requestId,
                connectionId,
                state.userId(),
                replyStreamId(connectionId),
                command.symbol(),
                null,
                null,
                null,
                null,
                null,
                command.orderId());
        offer(session, state, requestId, request);
    }

    /**
     * 先登记 pending 再 offer：ack 线程可能很快返回，先登记才不会把 ack 当成未知请求丢弃；
     * offer 失败时再撤销 pending。整个“登记 + offer”在同一连接锁内，保证同连接顺序。
     */
    private void offer(WebSocketSession session, TradeSession state, String requestId,
                       AeronOrderRequest request) {
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(properties.ackTimeoutMs());
        synchronized (state) {
            TradeSession.PendingAddResult addResult = state.addPending(requestId, deadlineNanos);
            if (addResult == TradeSession.PendingAddResult.DUPLICATE) {
                sendError(session, requestId, "REQUEST_ID_IN_PROGRESS");
                return;
            }
            if (addResult == TradeSession.PendingAddResult.LIMIT) {
                sendError(session, requestId, "RATE_LIMITED");
                return;
            }

            TradeAeronPublisher.OfferResult offerResult = publisher.offer(request);
            if (offerResult != TradeAeronPublisher.OfferResult.OFFERED) {
                state.removePending(requestId);
                sendError(session, requestId, offerError(offerResult));
            }
        }
    }

    /** ack 与 request 使用同一个分片下标；同一连接固定一个 ack poller。 */
    private int replyStreamId(long connectionId) {
        return aeronProperties.ackStreamId() + publisher.streamIndex(connectionId);
    }

    private TradeSession requireLogin(WebSocketSession session, long connectionId,
                                      String requestId) {
        TradeSession state = registry.get(connectionId);
        if (state == null) {
            sendError(session, requestId, "CONNECTION_NOT_REGISTERED");
            return null;
        }
        if (!state.isLoggedIn()) {
            sendError(session, requestId, "UNAUTHORIZED");
            return null;
        }
        return state;
    }

    private static String offerError(TradeAeronPublisher.OfferResult result) {
        return switch (result) {
            case BACK_PRESSURED -> "SERVER_BUSY";
            case NOT_CONNECTED, CLOSED -> "TRANSPORT_UNAVAILABLE";
            case ADMIN_ACTION, MAX_POSITION_EXCEEDED, ERROR -> "INTERNAL_ERROR";
            case OFFERED -> null;
        };
    }

    /** 按连接协议选择文本 JSON 或 SBE 二进制回执。 */
    private void send(WebSocketSession session, TradeServerMessage message) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        TradeSession state = registry.get(connectionId);
        if (state != null && state.isBinary()) {
            ByteBuffer frame = switch (message.type()) {
                case "loggedIn" -> binaryCodec.writeLoginAck(
                        message.userId() == null ? 0L : message.userId(), true, null);
                case "pong" -> binaryCodec.writePong();
                case "error" -> binaryCodec.writeError(message.requestId(), message.reason());
                default -> null;
            };
            if (frame != null) {
                sender.sendBinary(connectionId, frame);
                return;
            }
        }
        sender.send(connectionId, codec.write(message));
    }

    private void sendError(WebSocketSession session, String requestId, String reason) {
        send(session, TradeServerMessage.error(requestId, reason));
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null) {
            return null;
        }
        String normalized = requestId.strip();
        return normalized.isEmpty() ? null : normalized;
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        registry.remove(connectionId);
        log.debug("trade websocket closed: connectionId={}, status={}", connectionId, status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        long connectionId = TradeSessionRegistry.connectionId(session);
        registry.remove(connectionId);
        log.debug("trade websocket transport error: connectionId={}: {}",
                connectionId, exception.toString());
    }
}
