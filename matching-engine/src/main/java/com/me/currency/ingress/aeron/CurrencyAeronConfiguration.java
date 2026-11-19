package com.me.currency.ingress.aeron;

import com.me.aeron.AeronCodec;
import com.me.aeron.AeronMediaDriver;
import com.me.aeron.AeronProperties;
import com.me.currency.mq.OrderCancelCommandPublisher;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.OrderCancelAssembler;
import com.me.currency.service.OrderCommandAssembler;
import com.me.kafka.FatalState;
import com.me.snapshot.recovery.RecoveryState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 币币模块的 Aeron 消费者装配；{@code app.aeron.enabled=false} 时整组关闭。
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.aeron.enabled", havingValue = "true", matchIfMissing = true)
public class CurrencyAeronConfiguration {

    @Bean
    public AeronRequestProcessor aeronRequestProcessor(
            OrderCommandAssembler orderCommandAssembler,
            OrderCancelAssembler orderCancelAssembler,
            OrderCommandPublisher orderCommandPublisher,
            OrderCancelCommandPublisher orderCancelCommandPublisher,
            FatalState fatalState,
            RecoveryState recoveryState) {
        return new AeronRequestProcessor(
                orderCommandAssembler,
                orderCancelAssembler,
                orderCommandPublisher,
                orderCancelCommandPublisher,
                fatalState,
                recoveryState);
    }

    @Bean
    public AeronAckPublisher aeronAckPublisher(
            AeronMediaDriver mediaDriver,
            AeronProperties properties,
            AeronCodec codec) {
        return new AeronAckPublisher(mediaDriver, properties, codec);
    }

    @Bean
    public AeronOrderRequestPoller aeronOrderRequestPoller(
            AeronMediaDriver mediaDriver,
            AeronProperties properties,
            AeronCodec codec,
            AeronRequestProcessor processor,
            AeronAckPublisher ackPublisher,
            @Value("${app.aeron.stream-count:1}") int streamCount) {
        return new AeronOrderRequestPoller(
                mediaDriver, properties, codec, processor, ackPublisher, streamCount);
    }
}
