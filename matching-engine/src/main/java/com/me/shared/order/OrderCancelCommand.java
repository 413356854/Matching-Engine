package com.me.shared.order;

import java.util.Locale;

/**
 * 撤单命令，来自 Kafka topic {@code me.order.cancels-batch} 的跨模块契约。
 *
 * <p>由币币模块的撤单接口写入：HTTP 线程先非阻塞写入独立的撤单 Disruptor，聚合线程按
 * 「单 symbol 1000 条或 20ms」把 {@code Map<symbol, List<OrderCancelCommand>>} 中所有
 * symbol 的撤单分别封装成批量消息；委托模块消费后完成委托撤销与解冻清算。
 *
 * <p>{@code requestId} 是撤单请求标识（仅用于日志与审计）；{@code orderId} 由前端提供，
 * 是待撤销委托的唯一标识；{@code symbol} 统一转大写，用作 Kafka key，保证同一交易对的
 * 撤单始终落在同一分区、按序处理。{@code userId} 是发起撤单的用户，必须大于 0；
 * 委托模块只在它与委托归属一致时才撤销，不一致的命令直接丢弃。
 */
public record OrderCancelCommand(
        String requestId,
        long orderId,
        long userId,
        String symbol) {

    public OrderCancelCommand {
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
    }
}
