package com.me.websocket.trading.service;

import com.me.websocket.trading.vo.TradeServerMessage;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 扫描 /ws/trade 连接中等待 ack 超时的请求，通知客户端“结果未知，请查单”。
 *
 * <p>只清理 pending、不重试订单；客户端收到 {@code orderAckTimeout} 后应通过
 * {@code GET /api/v1/orders/by-request-id} 对账，不能自动重发。文本连接回 JSON，
 * 二进制连接回 SBE {@code AckTimeout}。
 */
public final class TradePendingTimeoutSweeper {

    private final TradeSessionRegistry registry;
    private final TradeSessionSender sender;
    private final TradeMessageCodec codec;
    private final TradeBinaryCodec binaryCodec;

    public TradePendingTimeoutSweeper(TradeSessionRegistry registry,
                                      TradeSessionSender sender,
                                      TradeMessageCodec codec,
                                      TradeBinaryCodec binaryCodec) {
        this.registry = registry;
        this.sender = sender;
        this.codec = codec;
        this.binaryCodec = binaryCodec;
    }

    @Scheduled(fixedDelayString = "${app.websocket.trading.ack-sweep-interval-ms:1000}")
    public void sweep() {
        long now = System.nanoTime();
        for (TradeSession state : registry.sessions()) {
            List<String> expired = state.removeExpired(now);
            if (expired.isEmpty()) {
                continue;
            }
            if (state.isBinary()) {
                for (String requestId : expired) {
                    sender.sendBinary(state.connectionId(),
                            binaryCodec.writeAckTimeout(requestId));
                }
            } else {
                for (String requestId : expired) {
                    sender.send(state.connectionId(),
                            codec.write(TradeServerMessage.ackTimeout(requestId)));
                }
            }
        }
    }
}
