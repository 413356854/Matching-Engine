package com.me.order.service;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * 委托模块两个外部入口的入口锁：下单和撤单。
 *
 * <p>入口锁与模块批次锁（{@link OrderBatchLock}）是两把不同的锁：
 * <ul>
 *   <li>入口 consumer 在进入 {@code OrderOperationService.executeCommands} /
 *       {@code executeCancels} 时先获取入口锁，再获取 orderLock；</li>
 *   <li>冻结回执等内部链路只获取 orderLock，不获取入口锁，因此快照持有入口锁等待
 *       内部链路排空时不会把内部级联也堵死。</li>
 * </ul>
 *
 * <p>获取顺序固定为「入口锁 → orderLock」。快照按
 * {@code placementLock → cancelLock → accountEntryLock → accountLock → orderLock}
 * 的顺序获取，避免与入口 consumer 形成锁环。
 */
@Component
public class OrderEntryLocks {

    /** 下单入口（{@code me.order.commands-batch}）。 */
    private final ReentrantLock placementLock = new ReentrantLock(true);

    /** 撤单入口（{@code me.order.cancels-batch}）。 */
    private final ReentrantLock cancelLock = new ReentrantLock(true);

    /** 供 {@code OrderOperationServiceImpl} 使用。 */
    public ReentrantLock placementLock() {
        return placementLock;
    }

    /** 供 {@code OrderOperationServiceImpl} 使用。 */
    public ReentrantLock cancelLock() {
        return cancelLock;
    }
}
