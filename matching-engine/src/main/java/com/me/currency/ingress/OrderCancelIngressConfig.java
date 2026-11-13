package com.me.currency.ingress;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 撤单入口 Disruptor 装配。
 *
 * <p>{@link OrderCancelIngressRing} 实现 {@code SmartLifecycle}，由 Spring 自动 start/stop；
 * 它是唯一向 {@code me.order.cancels-batch} 发送撤单批量消息的组件，与下单入口的
 * Disruptor 完全独立。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OrderCancelIngressProperties.class)
public class OrderCancelIngressConfig {

    @Bean
    public OrderCancelIngressRing orderCancelIngressRing(
            OrderCancelIngressProperties properties, OrderCancelBatchSender sender) {
        return new OrderCancelIngressRing(properties, sender);
    }
}
