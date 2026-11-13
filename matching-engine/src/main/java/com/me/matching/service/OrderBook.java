package com.me.matching.service;

import com.me.matching.dto.MatchResult;
import com.me.matching.entity.BookOrder;
import com.me.matching.vo.DepthSnapshot;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;

/**
 * 单个交易对的订单簿。
 *
 * <p>撮合写入由 {@code MatchingEngine} 的引擎对象锁串行化：下单撮合与撤单不会同时进入
 * 同一订单簿；生产形态下 Kafka key=symbol 还保证同一交易对的命令落在同一分区、按序消费。
 * 实现需要额外保证 REST 深度查询和撮合写入不会同时修改 / 遍历内部结构，具体同步策略
 * 由实现决定。
 */
public interface OrderBook {

    String symbol();

    /**
     * 处理一笔新委托：先撮合，未成交部分挂入订单簿。
     * 同一 {@code orderId} 重复投递时返回 {@link MatchResult#duplicate}，不会二次撮合。
     */
    MatchResult place(OrderPlacedEvent order);

    /**
     * 撤单：从订单簿移除指定 orderId 的挂单。
     *
     * <p>{@code side} / {@code type} / {@code price} 用于直接定位挂单所在的价格梯或市价簿；
     * 委托的方向、类型、价格在生命周期内不变。返回被移除的挂单（携带撤消时的剩余数量）；
     * 委托不在簿中（已全部成交、尚未挂单或重复撤单）时返回 {@code null}。已成交过的
     * orderId 仍会保留在去重集合中，防止重复下单事件在撤单后再次挂单。
     */
    BookOrder cancel(long orderId, OrderSide side, OrderType type, long price);

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
