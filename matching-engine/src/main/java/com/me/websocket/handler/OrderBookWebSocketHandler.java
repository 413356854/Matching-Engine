package com.me.websocket.handler;

import com.me.websocket.config.WebSocketProperties;
import com.me.websocket.dto.OrderBookSubscriptionCommand;
import com.me.websocket.service.OrderBookMessageCodec;
import com.me.websocket.service.OrderBookPushService;
import com.me.websocket.service.OrderBookSessionSender;
import com.me.websocket.service.OrderBookSubscriptionRegistry;
import com.me.websocket.vo.OrderBookMessage;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;

/**
 * {@code /ws/orderbook} 的消息处理：解析订阅指令、维护会话订阅并回执。
 *
 * <p>协议见 {@code com.me.websocket.package-info}。订阅成功后立即回执并推送首帧，
 * 之后由 {@link OrderBookPushService} 按固定周期推送。
 */
@Component
public class OrderBookWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderBookWebSocketHandler.class);

    private static final int MAX_SYMBOL_LENGTH = 32;
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("[A-Z0-9-]+");
    private static final String ACTION_SUBSCRIBE = "subscribe";
    private static final String ACTION_UNSUBSCRIBE = "unsubscribe";

    private final OrderBookSubscriptionRegistry registry;
    private final OrderBookMessageCodec codec;
    private final OrderBookPushService pushService;
    private final OrderBookSessionSender sender;
    private final WebSocketProperties properties;

    public OrderBookWebSocketHandler(
            OrderBookSubscriptionRegistry registry,
            OrderBookMessageCodec codec,
            OrderBookPushService pushService,
            OrderBookSessionSender sender,
            WebSocketProperties properties) {
        this.registry = registry;
        this.codec = codec;
        this.pushService = pushService;
        this.sender = sender;
        this.properties = properties;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        registry.register(session);
        log.debug("order book websocket connected: {}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        OrderBookSubscriptionCommand command;
        try {
            command = codec.readCommand(message.getPayload());
        } catch (JacksonException | IllegalArgumentException ex) {
            sendError(session, null, "MESSAGE_INVALID");
            return;
        }
        if (command == null) {
            sendError(session, null, "MESSAGE_INVALID");
            return;
        }

        String action = command.action() == null
                ? ""
                : command.action().strip().toLowerCase(Locale.ROOT);
        switch (action) {
            case ACTION_SUBSCRIBE -> handleSubscribe(session, command);
            case ACTION_UNSUBSCRIBE -> handleUnsubscribe(session);
            default -> sendError(session, null, "ACTION_INVALID");
        }
    }

    private void handleSubscribe(WebSocketSession session, OrderBookSubscriptionCommand command) {
        String symbol = normalizeSymbol(command.symbol());
        if (symbol.isEmpty()) {
            sendError(session, null, "SYMBOL_REQUIRED");
            return;
        }
        if (symbol.length() > MAX_SYMBOL_LENGTH || !SYMBOL_PATTERN.matcher(symbol).matches()) {
            sendError(session, symbol, "SYMBOL_INVALID");
            return;
        }

        int depth = command.depth() == null ? properties.defaultDepth() : command.depth();
        if (depth <= 0) {
            sendError(session, symbol, "DEPTH_INVALID");
            return;
        }
        depth = Math.min(depth, properties.maxDepth());

        WebSocketSession outbound = registry.subscribe(session, symbol, depth);
        sender.send(outbound, codec.write(OrderBookMessage.subscribed(symbol, depth)));
        pushService.pushNow(outbound, symbol, depth);
    }

    private void handleUnsubscribe(WebSocketSession session) {
        registry.unsubscribe(session);
        sender.send(session, codec.write(OrderBookMessage.unsubscribed()));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        registry.remove(session);
        log.debug("order book websocket closed: {}, status={}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        registry.remove(session);
        log.debug("order book websocket transport error, session={}: {}",
                session.getId(), exception.toString());
    }

    private void sendError(WebSocketSession session, String symbol, String reason) {
        sender.send(session, codec.write(OrderBookMessage.error(symbol, reason)));
    }

    private static String normalizeSymbol(String raw) {
        return raw == null ? "" : raw.strip().toUpperCase(Locale.ROOT);
    }
}
