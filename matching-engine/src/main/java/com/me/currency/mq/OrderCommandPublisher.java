package com.me.currency.mq;

import com.me.currency.ingress.OrderIngress;
import com.me.shared.order.OrderCommand;
import org.springframework.stereotype.Component;

/**
 * 下单命令入口：把委托命令提交到进程内 Disruptor。
 *
 * <p>{@code OrderIngressRing} 的单 handler 线程把订单按 symbol 放入
 * {@code Map<String, List>}，任一 symbol 达到 2000 条或 50ms 窗口到达时，把 map 中所有
 * symbol 的订单分别聚合为 {@code OrderCommandBatch} 发送到 {@code me.order.commands-batch}
 * （key=symbol，保证同一交易对按序处理）。委托模块的 {@code OrderCommandListener}
 * 是该 topic 的唯一消费者，也是系统唯一的下单入口。
 *
 * <p>提交只把命令放进 RingBuffer，不等待 Kafka；ring 满或未启动时同步抛异常，
 * 由 controller 返回 503。broker ack 失败的日志由批量发送器记录。
 *
 * <p>本包禁止依赖 {@code com.me.order}：下单接口只发布命令，不直接创建委托。
 */
@Component
public class OrderCommandPublisher {

    private final OrderIngress ingress;

    public OrderCommandPublisher(OrderIngress ingress) {
        this.ingress = ingress;
    }

    /** 非阻塞投递一条委托命令；只有 ring 未启动 / 已满才抛出异常，由 controller 返回 503。 */
    public void publish(OrderCommand command) {
        ingress.submit(command);
    }
}
