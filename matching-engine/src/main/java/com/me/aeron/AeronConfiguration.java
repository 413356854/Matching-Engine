package com.me.aeron;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Aeron 基础设施装配。
 *
 * <p>{@link AeronProperties} 和 {@link AeronCodec} 始终存在（编解码不依赖 driver）；
 * media driver 只在 {@code app.aeron.enabled=true} 时启动。依赖 driver 的消费者 / 发布者
 * 各自通过 {@code @ConditionalOnProperty} 关闭。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AeronProperties.class)
public class AeronConfiguration {

    @Bean
    @ConditionalOnProperty(name = "app.aeron.enabled", havingValue = "true", matchIfMissing = true)
    public AeronMediaDriver aeronMediaDriver(AeronProperties properties) {
        return new AeronMediaDriver(properties);
    }
}
