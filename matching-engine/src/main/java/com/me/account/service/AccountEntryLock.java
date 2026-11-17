package com.me.account.service;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * 账户模块外部入口锁：{@code me.account.commands}（充值 / 扣账等非冻结资金命令）。
 *
 * <p>它与账户模块批次锁（{@link AccountBatchLock}）是两把不同的锁：
 * <ul>
 *   <li>入口 consumer 在进入 {@code AccountOperationService.executeCommands} 时先获取
 *       本锁，再获取 accountLock；</li>
 *   <li>冻结命令、成交结算等内部链路只获取 accountLock，不获取本锁，因此快照持有
 *       入口锁等待内部 topic 排空时不会堵死级联。</li>
 * </ul>
 *
 * <p>获取顺序固定为「入口锁 → accountLock」。
 */
@Component
public class AccountEntryLock {

    private final ReentrantLock commandLock = new ReentrantLock(true);

    /** 供 {@code AccountOperationServiceImpl} 使用。 */
    public ReentrantLock reentrantLock() {
        return commandLock;
    }
}
