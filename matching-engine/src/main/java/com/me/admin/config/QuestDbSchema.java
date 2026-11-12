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

    private QuestDbSchema() {
    }
}
