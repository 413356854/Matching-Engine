package com.me.currency.ingress;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 下单入口 Disruptor 装配。
 *
 * <p>{@link OrderIngressRing} 实现 {@code SmartLifecycle}，由 Spring 自动 start/stop；
 * 它是唯一向 {@code me.order.commands-batch} 发送下单批量消息的组件。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OrderIngressProperties.class)
public class OrderIngressConfig {

    @Bean
    public OrderIngressRing orderIngressRing(OrderIngressProperties properties,
                                             OrderBatchSender sender) {
        return new OrderIngressRing(properties, sender);
    }
}
