package com.me.order.service;

import com.me.order.dto.OrderMatchResult;
import com.me.shared.order.OrderPlacedEvent;

/**
 * 委托侧撮合编排：把已支付委托同步送入进程内 {@code MatchingEngine}，完成成交清算，
 * 返回需要在 Kafka 监听事务内发布的行情与结算消息。
 *
 * <p>只能由 {@code OrderFreezeResultListener} 在处理冻结回执时单线程调用（监听器
 * {@code concurrency=1}）；方法会修改订单簿与委托状态，异常必须向上抛出以回滚监听器事务。
 *
 * <p>{@code sourcePartition/sourceOffset/sourceIndex} 是触发本次撮合的
 * {@code me.order.freeze-results-batch} 消息内位置，仅用于重复回执日志定位；演示版不做
 * 记录身份重放缓存，撮合引擎按 orderId 返回 duplicate 后不再产生输出。后续会补备份消息与
 * 内存状态恢复。
 */
public interface OrderMatchingService {

    OrderMatchResult match(OrderPlacedEvent order, int sourcePartition, long sourceOffset,
                           int sourceIndex);
}
