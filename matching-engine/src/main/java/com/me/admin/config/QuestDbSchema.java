package com.me.admin.config;

/**
 * QuestDB 表结构常量。
 *
 * <p>{@code user_id} / {@code currency} / {@code log_type} 建 SYMBOL 索引，支撑按用户、币种、
 * 日志类型查询列表；表按 {@code occurred_at} 天分区并使用 WAL，允许写入与查询并发。
 */
public final class QuestDbSchema {

    public static final String CREATE_FUNDS_FLOW_TABLE = """
            CREATE TABLE IF NOT EXISTS funds_flow (
                event_id STRING,
                request_id STRING,
                user_id SYMBOL INDEX CAPACITY 65536,
                currency SYMBOL INDEX CAPACITY 1024,
                log_type SYMBOL INDEX CAPACITY 1024,
                op SYMBOL,
                log_desc STRING,
                amount LONG,
                available_before LONG,
                available_after LONG,
                frozen_before LONG,
                frozen_after LONG,
                seq LONG,
                occurred_at TIMESTAMP,
                archived_at TIMESTAMP
            ) TIMESTAMP(occurred_at) PARTITION BY DAY WAL
            """;

    /**
     * 委托历史表：只归档撤单（{@code CANCELED}）与撮合完成（{@code FILLED}）两种终态。
     *
     * <p>指定时间戳 {@code final_at} 是委托进入终态的时间；委托模块保证同一委托的
     * {@code final_at} 不变，因此按 {@code (final_at, order_id)} DEDUP 后，Kafka 事务
     * 回滚重投不会产生重复行。{@code user_id} / {@code symbol} / {@code status} 建 SYMBOL
     * 索引，支撑下单前端按用户、管理后台按用户 / 交易对 / 状态查询。
     */
    public static final String CREATE_ORDER_HISTORY_TABLE = """
            CREATE TABLE IF NOT EXISTS ord_history (
                order_id LONG,
                request_id STRING,
                user_id SYMBOL INDEX CAPACITY 65536,
                symbol SYMBOL INDEX CAPACITY 1024,
                side SYMBOL,
                type SYMBOL,
                status SYMBOL INDEX CAPACITY 16,
                price LONG,
                quantity LONG,
                filled_quantity LONG,
                frozen_currency SYMBOL CAPACITY 1024,
                frozen_amount LONG,
                price_scale INT,
                quantity_scale INT,
                quote_scale INT,
                seq LONG,
                created_at TIMESTAMP,
                final_at TIMESTAMP,
                archived_at TIMESTAMP
            ) TIMESTAMP(final_at) PARTITION BY DAY WAL
            DEDUP UPSERT KEYS(final_at, order_id)
            """;

    private QuestDbSchema() {
    }
}
