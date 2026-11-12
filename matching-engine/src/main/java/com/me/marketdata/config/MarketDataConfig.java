package com.me.marketdata.config;

import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.service.impl.KlineAggregator;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 行情模块装配：把配置里的周期 label 解析为 {@link KlineInterval}，
 * 构造纯 JDK 的 {@link KlineAggregator}。
 *
 * <p>未知周期会在启动时直接失败，避免行情消费到第一条成交才发现配置错误。
 */
@Configuration
@EnableConfigurationProperties(MarketDataProperties.class)
public class MarketDataConfig {

    @Bean
    public KlineAggregator klineAggregator(MarketDataProperties properties) {
        List<KlineInterval> intervals = properties.klineIntervals().stream()
                .map(KlineInterval::of)
                .toList();
        return new KlineAggregator(intervals);
    }
}
