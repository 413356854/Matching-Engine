package com.me.account.ingress;

/**
 * RingBuffer 的可复用 slot。
 *
 * <p>只持有 task 引用，处理结果通过 task 的 future 返回，避免结果字段在 slot 复用时的
 * 可见性问题。每个 producer 同一时刻最多一个在途批次，因此不存在「slot 被覆盖但 batch
 * 尚未读取」的窗口。
 */
final class AccountIngressEvent {

    private AccountIngressTask task;

    AccountIngressTask task() {
        return task;
    }

    void setTask(AccountIngressTask task) {
        this.task = task;
    }
}
