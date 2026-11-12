/**
 * 跨模块委托契约：委托命令、下单事件、方向、类型、状态；委托、撮合、结算共用。
 *
 * <p>{@link com.me.shared.order.OrderCommand} 是 {@code me.order.commands-batch} 的消息契约：
 * 币币模块下单接口或外部生产者写入，委托模块的 Kafka 消费者是唯一接单入口。
 */
package com.me.shared.order;
