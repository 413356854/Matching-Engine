package com.me.currency.ingress;

import com.me.shared.order.OrderCancelCommand;

/**
 * HTTP 撤单到 Kafka 之间的进程内入口。
 *
 * <p>撤单接口只调用 {@link #submit}；命令进入独立的撤单 Disruptor 后，由唯一的聚合线程
 * 按 symbol 攒批，任一 symbol 达到 1000 条或 20ms 窗口到达时，把当前所有 symbol 的撤单
 * 分别封装成 {@code OrderCancelBatch} 批量消息。
 */
public interface OrderCancelIngress {

    /**
     * 非阻塞入队一条撤单命令。
     *
     * @throws IllegalStateException ring 未启动或已满；调用方（controller）应返回 503
     */
    void submit(OrderCancelCommand command);
}
