/**
 * admin 模块 Kafka 适配层：消费 me.funds-batch 资金流水、me.order.history-batch 委托历史
 * 与 me.match-pair-batch 撮合对，批量归档 QuestDB {@code funds_flow} / {@code ord_history} /
 * {@code user_trade}。
 */
package com.me.admin.mq;
