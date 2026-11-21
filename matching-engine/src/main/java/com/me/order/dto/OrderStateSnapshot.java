package com.me.order.dto;

import java.util.List;

/**
 * 委托模块的内存状态快照：内存中仍保留的委托（未完成 + {@code REJECTED}；终态委托在
 * 发布历史事件后已移除）+ 成交结算 requestId 生成器当前值。
 *
 * <p>{@code tradeSettlementIdGenerator} 必须随快照保存；恢复后从该值继续生成，
 * 才能保证资金流水的 requestId / eventId 不与快照之前的数据重复。
 */
public record OrderStateSnapshot(
        List<OrderSnapshot> orders,
        long tradeSettlementIdGenerator) {
}
