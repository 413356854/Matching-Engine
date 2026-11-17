/**
 * 机器人模块服务：币安行情客户端、自动下单调度与启动充值。
 *
 * <p>下单只通过币币模块的 {@code OrderCommandAssembler} + {@code OrderCommandPublisher}
 * 进入 Kafka 下单链路；本包不直接操作委托、账户或订单簿。
 */
package com.me.bot.service;
