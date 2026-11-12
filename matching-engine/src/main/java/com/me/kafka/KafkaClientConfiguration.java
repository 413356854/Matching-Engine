package com.me.kafka;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 注册 {@link KafkaClientProperties}；裸 client 的工厂、sink、REST 事务模板都是常规组件。
 */
@Configuration
@EnableConfigurationProperties(KafkaClientProperties.class)
public class KafkaClientConfiguration {
}
