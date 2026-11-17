package com.me.kafka;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费侧全局 fatal 状态。
 *
 * <p>任一 consumer 出现不可恢复异常时进入 fatal：
 * <ol>
 *   <li>置位 fatal，HTTP 入口立即返回 503，不再对外提供可能已经脏掉的内存状态；</li>
 *   <li>通过 {@link #setShutdownAction(Runnable)} 注册的动作通知全部 runner 停止，
 *       避免同 group 的其他 runner 接手分区、在同一 JVM 里重放失败批次；</li>
 *   <li>进程保持存活，等待外部监控重启。重启后内存状态丢弃，从快照/日志恢复。</li>
 * </ol>
 *
 * <p>监控接入前，fatal 只体现在 error 日志和 HTTP 503 上，进程不会自动退出。
 */
@Component
public class FatalState {

    private static final Logger log = LoggerFactory.getLogger(FatalState.class);

    private final AtomicBoolean fatal = new AtomicBoolean(false);
    private final AtomicReference<Throwable> cause = new AtomicReference<>();
    private volatile Runnable shutdownAction = () -> { };

    /**
     * 由 {@link KafkaConsumerManager} 注册全局停止动作。
     *
     * <p>该动作只负责给所有 runner 发停止信号，不能阻塞等待 runner 线程结束：
     * 它可能由某个 runner 线程自己调用。
     */
    public void setShutdownAction(Runnable shutdownAction) {
        this.shutdownAction = shutdownAction == null ? () -> { } : shutdownAction;
    }

    /**
     * 标记 fatal 并触发一次全局停止；重复调用只保留第一次的 cause。
     *
     * @return true 表示本次调用首次进入 fatal
     */
    public boolean markFatal(String runner, Throwable error) {
        cause.compareAndSet(null, error);
        if (!fatal.compareAndSet(false, true)) {
            return false;
        }
        log.error("[{}] fatal consumer error; stopping all consumers and awaiting process restart",
                runner, error);
        try {
            shutdownAction.run();
        } catch (RuntimeException shutdownError) {
            log.error("failed to stop all consumers after fatal error", shutdownError);
        }
        return true;
    }

    public boolean isFatal() {
        return fatal.get();
    }

    /** 首次触发 fatal 的异常；未触发时为 {@code null}。 */
    public Throwable cause() {
        return cause.get();
    }
}
