package com.me.websocket.config;

import com.me.websocket.handler.OrderBookWebSocketHandler;
import java.util.Arrays;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * WebSocket 接入配置：注册盘口推送端点，并打开定时推送所需的调度器。
 *
 * <p>端点 {@code /ws/orderbook} 使用原生 WebSocket（不引入 STOMP/SockJS），
 * 前端通过 Vite 代理或同源直连。{@code @EnableScheduling} 用于
 * {@code OrderBookPushService} 的固定频率推送。
 */
@Configuration
@EnableWebSocket
@EnableScheduling
@EnableConfigurationProperties(WebSocketProperties.class)
public class WebSocketConfig implements WebSocketConfigurer {

    /** 盘口订阅端点；前端与 Vite 代理配置保持一致。 */
    public static final String ORDER_BOOK_PATH = "/ws/orderbook";

    private final OrderBookWebSocketHandler orderBookWebSocketHandler;
    private final WebSocketProperties properties;

    public WebSocketConfig(
            OrderBookWebSocketHandler orderBookWebSocketHandler,
            WebSocketProperties properties) {
        this.orderBookWebSocketHandler = orderBookWebSocketHandler;
        this.properties = properties;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(orderBookWebSocketHandler, ORDER_BOOK_PATH)
                .setAllowedOriginPatterns(originPatterns());
    }

    private String[] originPatterns() {
        String[] patterns = Arrays.stream(properties.allowedOriginPatterns().split(","))
                .map(String::strip)
                .filter(pattern -> !pattern.isEmpty())
                .toArray(String[]::new);
        return patterns.length == 0 ? new String[]{"*"} : patterns;
    }
}
