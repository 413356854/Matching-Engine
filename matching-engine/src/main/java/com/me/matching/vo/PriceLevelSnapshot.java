package com.me.matching.vo;

import java.util.List;

/**
 * 订单簿一个价格档位的快照；{@code orders} 保持该档位的 FIFO 顺序。
 */
public record PriceLevelSnapshot(
        long price,
        List<BookOrderSnapshot> orders) {
}
