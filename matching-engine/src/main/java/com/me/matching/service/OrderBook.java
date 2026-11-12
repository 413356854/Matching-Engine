package com.me.matching.service;

import com.me.matching.dto.MatchResult;
import com.me.matching.entity.BookOrder;
import com.me.matching.vo.DepthSnapshot;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;

/**
 * 单个交易对的订单簿。
 *
 * <p>撮合写入仍由单线程驱动：生产形态下 Kafka key=symbol 保证同一交易对落在同一分区、
 * 由单线程消费（见 README「七个关键决策」）。实现需要额外保证 REST 深度查询和撮合写入
 * 不会同时修改 / 遍历内部结构，具体同步策略由实现决定。
 */
public interface OrderBook {

    String symbol();

    /**
     * 处理一笔新委托：先撮合，未成交部分挂入订单簿。
     * 同一 {@code orderId} 重复投递时返回 {@link MatchResult#duplicate}，不会二次撮合。
     */
    MatchResult place(OrderPlacedEvent order);

    /**
     * 只读深度快照：按价格聚合前 {@code levels} 个价位。
     *
     * @param levels 请求的价位数量，1..100，超出范围由实现夹取
     */
    DepthSnapshot depth(int levels);

    /** 指定方向上仍在簿中的限价单数量。 */
    int limitOrderCount(OrderSide side);

    /** 指定方向上仍在簿中的市价单数量。 */
    int marketOrderCount(OrderSide side);

    /** 按 orderId 查找挂单，找不到返回 {@code null}（测试 / 运维查看用）。 */
    BookOrder find(long orderId);
}
