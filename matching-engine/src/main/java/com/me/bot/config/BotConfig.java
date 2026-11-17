package com.me.bot.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 机器人模块配置装配：注册 {@link BotProperties}。
 *
 * <p>订单调度、行情连接与启动充值都由各自组件的生命周期管理，不在这里创建线程。
 */
@Configuration
@EnableConfigurationProperties(BotProperties.class)
public class BotConfig {
}
