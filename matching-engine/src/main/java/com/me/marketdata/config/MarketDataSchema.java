package com.me.marketdata.config;

/**
 * 行情模块 QuestDB 表结构。
 *
 * <h2>表设计</h2>
 * <ul>
 *   <li>{@code md_trade}：每笔成交一行，指定时间戳 {@code executed_at}，
 *       按 {@code (executed_at, symbol, taker_order_id, maker_order_id)} 去重，
 *       消费重投不会产生重复成交。</li>
 *   <li>{@code md_kline}：每笔成交写一条完整 K 线快照，按
 *       {@code (bucket_start, symbol, bar_interval)} 去重（后写覆盖），等价于按 bucket upsert。
 *       {@code source_partition/source_offset} 是消费端幂等水位。</li>
 *   <li>{@code md_kline_anchor}：每个 {@code (symbol, bar_interval)} 一行锚点，记录已经计算到
 *       的最新 bucket，用于重启 / 缓存未命中时恢复聚合位置。QuestDB 的 DEDUP 键必须包含指定
 *       时间戳，因此用固定为 epoch 0 的 {@code anchor_key} 作指定时间戳：同一
 *       {@code (symbol, bar_interval)} 反复写入时后写覆盖，表里始终只有一行。</li>
 * </ul>
 *
 * <p>DEDUP 要求：仅 WAL 表可用；指定时间戳必须在 UPSERT KEYS 中；键列必须是定长类型
 * （SYMBOL / LONG / INT 可用，STRING / VARCHAR 不可用）。
 */
public final class MarketDataSchema {

    public static final String CREATE_MD_TRADE_TABLE = """
            CREATE TABLE IF NOT EXISTS md_trade (
                trade_id LONG,
                symbol SYMBOL INDEX CAPACITY 1024,
                taker_order_id LONG,
                maker_order_id LONG,
                price LONG,
                quantity LONG,
                amount LONG,
                taker_side SYMBOL,
                executed_at TIMESTAMP,
                archived_at TIMESTAMP
            ) TIMESTAMP(executed_at) PARTITION BY DAY WAL
            DEDUP UPSERT KEYS(executed_at, symbol, taker_order_id, maker_order_id)
            """;

    public static final String CREATE_MD_KLINE_TABLE = """
            CREATE TABLE IF NOT EXISTS md_kline (
                symbol SYMBOL INDEX CAPACITY 1024,
                bar_interval SYMBOL,
                bucket_start TIMESTAMP,
                open_price LONG,
                high_price LONG,
                low_price LONG,
                close_price LONG,
                volume LONG,
                amount LONG,
                trade_count LONG,
                source_partition INT,
                source_offset LONG,
                updated_at TIMESTAMP
            ) TIMESTAMP(bucket_start) PARTITION BY DAY WAL
            DEDUP UPSERT KEYS(bucket_start, symbol, bar_interval)
            """;

    /**
     * 锚点表。{@code anchor_key} 固定为 epoch 0，只用于满足 DEDUP 必须包含指定时间戳的要求，
     * 让 {@code (symbol, bar_interval)} 成为实际主键；{@code updated_at} 记录真实写入时间，
     * 读取时按它倒序取最新一行，兼容 DEDUP 尚未 apply 的短暂窗口。
     */
    public static final String CREATE_MD_KLINE_ANCHOR_TABLE = """
            CREATE TABLE IF NOT EXISTS md_kline_anchor (
                symbol SYMBOL INDEX CAPACITY 1024,
                bar_interval SYMBOL,
                bucket_start TIMESTAMP,
                updated_at TIMESTAMP,
                anchor_key TIMESTAMP
            ) TIMESTAMP(anchor_key) PARTITION BY DAY WAL
            DEDUP UPSERT KEYS(anchor_key, symbol, bar_interval)
            """;

    private MarketDataSchema() {
    }
}
