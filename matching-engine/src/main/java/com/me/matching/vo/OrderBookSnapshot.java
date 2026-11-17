package com.me.matching.vo;

import java.util.List;

/**
 * 单个交易对订单簿的完整内存快照。
 *
 * <p>{@code bids} / {@code asks} 保持价格梯的自然顺序（买盘价格降序、卖盘升序），
 * 每个档位内的挂单保持 FIFO 顺序。{@code seenOrderIds} 与 {@code nextTradeId}
 * 也必须保存：前者防止恢复后重复冻结回执把已成交委托重新挂回订单簿，后者保证
 * tradeId 不与历史成交重复。
 */
public record OrderBookSnapshot(
        String symbol,
        List<PriceLevelSnapshot> bids,
        List<PriceLevelSnapshot> asks,
        List<BookOrderSnapshot> marketBids,
        List<BookOrderSnapshot> marketAsks,
        List<Long> seenOrderIds,
        long nextTradeId) {
}
