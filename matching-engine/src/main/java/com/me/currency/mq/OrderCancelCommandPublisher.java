package com.me.currency.mq;

import com.me.currency.ingress.OrderCancelIngress;
import com.me.shared.order.OrderCancelCommand;
import org.springframework.stereotype.Component;

/**
 * 撤单命令入口：把撤单命令提交到独立的撤单 Disruptor。
 *
 * <p>{@code OrderCancelIngressRing} 的单 handler 线程按 symbol 攒批，任一 symbol 达到
 * 1000 条或 20ms 窗口到达时，把 map 中所有 symbol 的撤单分别封装成
 * {@code OrderCancelBatch}，按 symbol 分组发送到 {@code me.order.cancels-batch}
 * （key=symbol）。委托模块的 {@code OrderCancelListener} 是该 topic 的唯一消费者。
 *
 * <p>提交只把命令放进 RingBuffer，不等待 Kafka；ring 满或未启动时同步抛异常，
 * 由 controller 返回 503。broker ack 失败的日志由批量发送器记录。
 *
 * <p>本包禁止依赖 {@code com.me.order}：撤单接口只发布命令，不直接修改委托。
 */
@Component
public class OrderCancelCommandPublisher {

    private final OrderCancelIngress ingress;

    public OrderCancelCommandPublisher(OrderCancelIngress ingress) {
        this.ingress = ingress;
    }

    /** 非阻塞投递一条撤单命令；只有 ring 未启动 / 已满才抛出异常，由 controller 返回 503。 */
    public void publish(OrderCancelCommand command) {
        ingress.submit(command);
    }
}
