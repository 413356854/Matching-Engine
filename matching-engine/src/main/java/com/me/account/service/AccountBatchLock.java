package com.me.account.service;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * 账户模块内部的批次锁。
 *
 * <p>锁属于账户模块，不放到 shared，也不由委托模块共享；将来拆分部署时随账户模块一起搬走。
 *
 * <p>由 {@link com.me.account.service.impl.AccountOperationServiceImpl} 在自己的
 * {@code execute*} 方法内获取：批次的状态修改、seq 分配、输出发布和
 * {@code output.enqueue()} 都在锁内完成。consumer runner 不再持有这把锁。
 */
@Component
public class AccountBatchLock {

    private final ReentrantLock lock = new ReentrantLock(true);

    /** 供 AccountOperationService 使用的可重入公平锁。 */
    public ReentrantLock reentrantLock() {
        return lock;
    }
}
