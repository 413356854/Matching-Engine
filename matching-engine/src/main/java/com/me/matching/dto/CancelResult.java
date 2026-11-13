package com.me.matching.dto;

/**
 * 一笔撮合撤消单的处理结果。
 *
 * <p>{@code removed=true} 表示挂单已从订单簿移除，{@code removedQuantity} 是撤消时的剩余
 * 数量（未成交部分）；{@code removed=false} 表示该委托当前不在订单簿中（已全部成交、
 * 尚未挂单或重复撤单），{@code reason} 记录原因。
 */
public record CancelResult(
        long orderId,
        String symbol,
        boolean removed,
        long removedQuantity,
        String reason) {

    public static CancelResult canceled(long orderId, String symbol, long removedQuantity) {
        return new CancelResult(orderId, symbol, true, removedQuantity, null);
    }

    public static CancelResult notFound(long orderId, String symbol) {
        return new CancelResult(orderId, symbol, false, 0L, "ORDER_NOT_RESTING");
    }
}
