package com.me.websocket.trading.config;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.websocket.config.WebSocketProperties;
import com.me.websocket.trading.handler.TradeWebSocketHandler;
import com.me.websocket.trading.service.TradeAckPoller;
import com.me.websocket.trading.service.TradeAeronPublisher;
import com.me.websocket.trading.service.TradeBinaryCodec;
import com.me.websocket.trading.service.TradeMessageCodec;
import com.me.websocket.trading.service.TradePendingTimeoutSweeper;
import com.me.websocket.trading.service.TradeSessionRegistry;
import com.me.websocket.trading.service.TradeSessionSender;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * /ws/trade 交易端点装配；Aeron 或交易端点任一关闭时整组不创建。
 *
 * <p>WS 模块不依赖 {@code com.me.currency} / {@code com.me.order}：订单只通过 Aeron
 * 交给币币模块，ack 通过 Aeron 返回。后续拆分 WS 进程时，这一组 bean 和
 * {@code app.websocket.trading.*} 配置可以整体迁移。
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        name = {"app.aeron.enabled", "app.websocket.trading.enabled"},
        havingValue = "true",
        matchIfMissing = true)
public class TradingConfiguration {

    @Bean
    public TradeSessionRegistry tradeSessionRegistry(
            TradingProperties properties, WebSocketProperties webSocketProperties) {
        return new TradeSessionRegistry(properties, webSocketProperties);
    }

    @Bean
    public TradeSessionSender tradeSessionSender(TradeSessionRegistry registry) {
        return new TradeSessionSender(registry);
    }

    @Bean
    public TradeBinaryCodec tradeBinaryCodec() {
        return new TradeBinaryCodec();
    }

    @Bean
    public TradeAeronPublisher tradeAeronPublisher(
            AeronMediaDriver mediaDriver, AeronProperties properties, AeronCodec codec,
            @Value("${app.aeron.stream-count:1}") int streamCount) {
        return new TradeAeronPublisher(mediaDriver, properties, codec, streamCount);
    }

    @Bean
    public TradeAckPoller tradeAckPoller(
            AeronMediaDriver mediaDriver,
            AeronProperties properties,
            AeronCodec codec,
            TradeMessageCodec messageCodec,
            TradeBinaryCodec binaryCodec,
            TradingProperties tradingProperties,
            TradeSessionRegistry registry,
            TradeSessionSender sender,
            @Value("${app.aeron.stream-count:1}") int streamCount) {
        return new TradeAckPoller(
                mediaDriver, properties, codec, messageCodec, binaryCodec, registry, sender,
                streamCount, tradingProperties.ackBatchSize(),
                tradingProperties.maxMessageBytes());
    }

    @Bean
    public TradePendingTimeoutSweeper tradePendingTimeoutSweeper(
            TradeSessionRegistry registry,
            TradeSessionSender sender,
            TradeMessageCodec codec,
            TradeBinaryCodec binaryCodec) {
        return new TradePendingTimeoutSweeper(registry, sender, codec, binaryCodec);
    }

    @Bean
    public TradeWebSocketHandler tradeWebSocketHandler(
            TradingProperties properties,
            AeronProperties aeronProperties,
            TradeMessageCodec codec,
            TradeBinaryCodec binaryCodec,
            TradeSessionRegistry registry,
            TradeSessionSender sender,
            TradeAeronPublisher publisher) {
        return new TradeWebSocketHandler(
                properties, aeronProperties, codec, binaryCodec, registry, sender, publisher);
    }
}
