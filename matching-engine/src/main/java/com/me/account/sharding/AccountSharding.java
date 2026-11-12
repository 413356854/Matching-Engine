package com.me.account.sharding;

/**
 * 账户写入分片：userId → slot（0..{@link #SLOTS}-1）。
 *
 * <p>账户锁数组、账户输入 topic（冻结 / 清算 / 充值）的 Kafka partition 都使用同一个
 * slot，保证同一用户的所有写操作落到同一把锁、同一个 consumer：
 *
 * <ul>
 *   <li>服务端的正确性只依赖 {@code slot(userId)} 与锁数组，不依赖 Kafka 实际分区；</li>
 *   <li>Kafka partition 与 slot 对齐只是为了让同一 slot 的消息由同一个 consumer 处理，
 *       避免跨 consumer 争锁。</li>
 * </ul>
 *
 * <p>userId 为正数时 {@code Long.hashCode} 与 {@code (int) userId} 等价；使用 hashCode
 * 是为了兼容高位非零的雪花类 userId。{@code SLOTS} 必须与 topic 分区数和账户锁数组长度一致。
 */
public final class AccountSharding {

    /** 账户写分片数；与账户输入 topic 的分区数一致。 */
    public static final int SLOTS = 8;

    public static int slot(long userId) {
        return Math.floorMod(Long.hashCode(userId), SLOTS);
    }

    private AccountSharding() {
    }
}
