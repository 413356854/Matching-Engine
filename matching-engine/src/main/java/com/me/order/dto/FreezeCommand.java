package com.me.order.dto;

import com.me.account.dto.FundsCommand;

/**
 * 一条待发送的下单冻结命令及其 symbol。
 *
 * <p>{@code symbol} 只用于 Kafka key 与批次分组；命令本身由账户模块消费。
 */
public record FreezeCommand(String symbol, FundsCommand command) {
}
