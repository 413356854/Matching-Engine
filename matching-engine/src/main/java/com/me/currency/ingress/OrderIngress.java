package com.me.currency.ingress;

import com.me.shared.order.OrderCommand;

/**
 * HTTP 下单到 Kafka 之间的进程内入口。
 *
 * <p>下单接口只调用 {@link #submit}；命令进入 Disruptor 后由唯一的聚合线程按 symbol 放入
 * {@code Map<String, List>}，任一 symbol 达到 2000 条或 50ms 窗口到达时，把 map 中所有
 * symbol 的订单分别组装成 {@code OrderCommandBatch} 批量消息。
 */
public interface OrderIngress {

    /**
     * 非阻塞入队一条委托命令。
     *
     * @throws IllegalStateException ring 未启动或已满；调用方（controller）应返回 503
     */
    void submit(OrderCommand command);
}
