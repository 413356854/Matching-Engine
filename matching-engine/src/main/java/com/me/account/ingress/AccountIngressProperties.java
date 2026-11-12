package com.me.account.ingress;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * account 消费入口（Disruptor）配置。
 *
 * @param disruptorEnabled true 时启用 ring；默认 false，走「poll 线程直连 listener」
 * @param ringSize         ring 槽位数，必须是 2 的幂；每个槽位只放一个批次引用
 * @param handlerTimeoutMs poll 线程等待 handler 处理完一批的上限
 */
@ConfigurationProperties("app.account.ingress")
public record AccountIngressProperties(
        @DefaultValue("false") boolean disruptorEnabled,
        @DefaultValue("1024") int ringSize,
        @DefaultValue("30000") long handlerTimeoutMs) {
}
