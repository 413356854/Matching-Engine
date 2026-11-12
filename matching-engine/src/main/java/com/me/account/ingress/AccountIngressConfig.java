package com.me.account.ingress;

import com.me.account.mq.AccountCommandListener;
import com.me.account.mq.AccountSettlementListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * account Disruptor 入口装配。
 *
 * <p>默认关闭：{@code app.account.ingress.disruptor-enabled} 缺省或为 false 时不创建 ring
 * 和两个 processor，{@code KafkaConsumerConfiguration} 走直连 listener。
 * 压测对比需要时显式设置 {@code --app.account.ingress.disruptor-enabled=true}。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AccountIngressProperties.class)
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class AccountIngressConfig {

    @Bean
    @ConditionalOnProperty(name = "app.account.ingress.disruptor-enabled",
            havingValue = "true", matchIfMissing = false)
    public AccountIngressRing accountIngressRing(
            AccountIngressProperties properties,
            AccountCommandListener commandListener,
            AccountSettlementListener settlementListener) {
        return new AccountIngressRing(properties, commandListener, settlementListener);
    }

    @Bean
    @ConditionalOnProperty(name = "app.account.ingress.disruptor-enabled",
            havingValue = "true", matchIfMissing = false)
    public AccountCommandRingProcessor accountCommandRingProcessor(
            AccountIngressRing ring,
            AccountCommandListener listener) {
        return new AccountCommandRingProcessor(ring, listener);
    }

    @Bean
    @ConditionalOnProperty(name = "app.account.ingress.disruptor-enabled",
            havingValue = "true", matchIfMissing = false)
    public AccountSettlementRingProcessor accountSettlementRingProcessor(
            AccountIngressRing ring,
            AccountSettlementListener listener) {
        return new AccountSettlementRingProcessor(ring, listener);
    }
}
