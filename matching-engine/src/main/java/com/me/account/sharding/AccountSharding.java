package com.me.account.sharding;

/**
 * 账户写入分片：userId → slot（0..{@link #SLOTS}-1）。
 *
 * <p>账户锁数组固定为 {@link #SLOTS} 把，同一用户的冻结、清算、充值都使用同一把
 * {@code slot(userId)} 锁，保证不会并发修改同一账户。Kafka 分区策略按 topic 区分：
 *
 * <ul>
 *   <li>{@code me.account.commands}（充值 / 扣账）与
 *       {@code me.account.settlements-batch}（清算）显式使用 {@code partition=slot}，
 *       让同一 slot 的消息由同一个 consumer 处理，避免跨 consumer 争锁；</li>
 *   <li>{@code me.account.order-freeze-commands-batch}（冻结）按 {@code key=symbol} 分区，
 *       以保持同一 symbol 的顺序；消费端在批内再按 {@code slot(userId)} 分组并发处理，
 *       正确性仍由 slot 锁保证；</li>
 *   <li>服务端的正确性只依赖 {@code slot(userId)} 与锁数组，不依赖 Kafka 实际分区。</li>
 * </ul>
 *
 * <p>userId 为正数时 {@code Long.hashCode} 与 {@code (int) userId} 等价；使用 hashCode
 * 是为了兼容高位非零的雪花类 userId。{@code SLOTS} 必须与账户锁数组长度一致，按 slot
 * 显式分区的 topic 分区数也应等于 {@code SLOTS}。
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
