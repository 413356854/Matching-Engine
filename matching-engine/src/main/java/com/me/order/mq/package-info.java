/**
 * 委托模块 Kafka 适配层：消费委托命令与冻结回执，发送冻结命令、行情记录、双边成交结算命令
 * 与终态委托历史（撤单 / 撮合完成 → {@code me.order.history-batch}）。
 *
 * <p>冻结成功回执会直接触发进程内撮合，不再经过 {@code me.orders} / {@code me.trades} topic。
 */
package com.me.order.mq;
