/**
 * 委托模块 Kafka 适配层：消费委托命令与冻结回执，发送冻结命令、行情记录与双边成交结算命令。
 *
 * <p>冻结成功回执会直接触发进程内撮合，不再经过 {@code me.orders} / {@code me.trades} topic。
 */
package com.me.order.mq;
