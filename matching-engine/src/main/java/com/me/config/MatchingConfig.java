package com.me.config;

import com.me.matching.service.MatchingEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 撮合内核装配。
 *
 * <p>{@link MatchingEngine} 本身零 Spring 依赖，由应用配置层注册为 Bean；
 * 这样内核可以脱离 Spring 独立编译、压测与移植。运行时由委托模块的
 * {@code OrderMatchingService} 在冻结回执流程中直接调用，不再经过 Kafka。
 */
@Configuration
public class MatchingConfig {

    @Bean
    public MatchingEngine matchingEngine() {
        return new MatchingEngine();
    }
}
