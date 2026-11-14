package com.me.order.service;

import com.me.matching.dto.MatchResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelMatchResult;
import com.me.order.dto.OrderMatchResult;
import com.me.shared.order.OrderPlacedEvent;
import java.util.List;

/**
 * 委托侧撮合编排：把已支付委托送入进程内 {@code MatchingEngine}，完成成交清算，
 * 返回需要在 Kafka 监听事务内发布的撮合对与结算消息；撤单时把一批撤消单聚合后送入
 * 同一个撮合引擎，收集被撤销的挂单。
 *
 * <p><b>冻结回执两段式</b>：{@link #matchBatch} 聚合整批已支付委托调用撮合引擎并返回
 * 成交对，{@link #clear} 对每笔委托生成撮合对与结算命令。调用方（{@code OrderFreezeResultListener}）
 * 在引擎对象锁内完成两段，再把 {@code clear} 分发到订单更新线程池并发执行，
 * 既保证撮合输入顺序，又允许成交清算并行。
 *
 * <p><b>调用约束</b>：下单由 {@code OrderFreezeResultListener} 调用；撤单由
 * {@code OrderCancelListener} 调用。两个入口可能并发（不同 topic、不同 consumer），
 * 因此撮合引擎的 {@code place} / {@code cancel} 都使用引擎对象锁串行化。
 *
 * <p>方法会修改订单簿与委托状态，异常必须向上抛出以回滚监听器事务。
 *
 * <p>演示版不做记录身份重放缓存，撮合引擎按 orderId 返回 duplicate 后不再产生输出。
 * 后续会补备份消息与内存状态恢复。
 */
public interface OrderMatchingService {

    /**
     * 批量撮合：在引擎对象锁内按列表顺序把已支付委托送入撮合引擎，返回与输入一一对应的
     * 成交对。已处于撤单中的委托不会挂单，返回 {@link MatchResult#duplicate}。
     */
    List<MatchResult> matchBatch(List<OrderPlacedEvent> orders);

    /**
     * 成交清算：对一笔已撮合委托执行成交清算（含市价买入完成退款），生成撮合对与结算命令。
     * 可在订单更新线程池中并发调用；与撤单的互斥由调用方持有的引擎对象锁临界区保证。
     */
    OrderMatchResult clear(OrderPlacedEvent order, MatchResult result);

    /**
     * 批量撤单：按批次聚合撤消单后调用撮合引擎，返回每笔撤单的移除结果与撤单标记结果。
     *
     * <p>整批撤消单在同一个引擎对象锁临界区内依次执行：先由撮合引擎移除挂单，再由
     * {@code OrderService.beginCancel} 标记“撤单中”，两者原子地完成，避免与下单撮合交错。
     * 不在订单簿中的委托返回 {@code removed=false}，只要标记成功，调用方仍会继续推进
     * 委托状态并解冻资金。
     */
    List<OrderCancelMatchResult> cancel(List<OrderCancelContext> contexts);
}
