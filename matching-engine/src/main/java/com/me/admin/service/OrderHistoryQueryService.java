package com.me.admin.service;

import com.me.admin.entity.OrderHistoryRow;
import com.me.shared.order.OrderStatus;
import java.util.List;

/**
 * 管理端委托历史查询：按用户 / 交易对 / 终态筛选，按最终时间倒序。
 */
public interface OrderHistoryQueryService {

    /**
     * @param userId 用户 ID；{@code null} 表示不筛选
     * @param symbol 交易对（大写）；{@code null} 表示不筛选
     * @param status 终态（CANCELED / FILLED）；{@code null} 表示不筛选
     * @param limit  1..1000，超出范围由实现夹取
     */
    List<OrderHistoryRow> list(Long userId, String symbol, OrderStatus status, int limit);
}
