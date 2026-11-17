package com.me.order.service;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * 委托模块内部的批次锁。
 *
 * <p>锁属于委托模块，不放到 shared，也不由账户模块共享；将来拆分部署时随委托模块一起搬走。
 *
 * <p>由 {@link com.me.order.service.impl.OrderOperationServiceImpl} 在自己的
 * {@code execute*} 方法内获取：批次的状态修改、seq 分配、输出发布和
 * {@code output.enqueue()} 都在锁内完成。consumer runner 不再持有这把锁。
 */
@Component
public class OrderBatchLock {

    private final ReentrantLock lock = new ReentrantLock(true);

    /** 供 OrderOperationService 使用的可重入公平锁。 */
    public ReentrantLock reentrantLock() {
        return lock;
    }
}
