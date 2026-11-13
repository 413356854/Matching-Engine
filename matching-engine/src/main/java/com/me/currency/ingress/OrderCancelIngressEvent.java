package com.me.currency.ingress;

import com.me.shared.order.OrderCancelCommand;
import java.util.concurrent.CompletableFuture;

/**
 * 撤单 RingBuffer 的可复用 slot。
 *
 * <p>正常事件携带一条撤单命令；定时 tick 事件没有命令，只用来触发聚合线程检查
 * 「20ms 窗口已到」；stop 事件是停机屏障：handler 处理到它时说明之前的撤单都已入队，
 * flush 后完成 future。handler 处理完必须 {@link #clear()}，避免 slot 复用时持有上一批引用。
 */
final class OrderCancelIngressEvent {

    private OrderCancelCommand command;
    private boolean tick;
    private CompletableFuture<Void> stop;

    OrderCancelCommand command() {
        return command;
    }

    boolean tick() {
        return tick;
    }

    CompletableFuture<Void> stop() {
        return stop;
    }

    void set(OrderCancelCommand command) {
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
