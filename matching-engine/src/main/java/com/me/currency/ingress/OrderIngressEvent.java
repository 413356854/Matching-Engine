package com.me.currency.ingress;

import com.me.shared.order.OrderCommand;
import java.util.concurrent.CompletableFuture;

/**
 * RingBuffer 的可复用 slot。
 *
 * <p>正常事件携带一条委托命令；定时 tick 事件没有命令，只用来
 * 触发聚合线程检查「20ms 窗口已到」；stop 事件是停机屏障：handler 处理到它时
 * 说明它之前的命令都已入队，flush 后完成 future。handler 处理完必须 {@link #clear()}，
 * 避免 slot 复用时持有上一批的引用。
 */
final class OrderIngressEvent {

    private OrderCommand command;
    private boolean tick;
    private CompletableFuture<Void> stop;

    OrderCommand command() {
        return command;
    }

    boolean tick() {
        return tick;
    }

    CompletableFuture<Void> stop() {
        return stop;
    }

    void set(OrderCommand command) {
        this.command = command;
        this.tick = false;
        this.stop = null;
    }

    void setTick() {
        this.command = null;
        this.tick = true;
        this.stop = null;
    }

    void setStop(CompletableFuture<Void> stop) {
        this.command = null;
        this.tick = false;
        this.stop = stop;
    }

    void clear() {
        this.command = null;
        this.tick = false;
        this.stop = null;
    }
}
