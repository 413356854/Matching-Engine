package com.me.admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * QuestDB 连接配置（{@code app.questdb.*}）。
 *
 * <p>{@code jdbcUrl} 走 PostgreSQL wire，用于查询、DDL 与（{@code writeMode=jdbc} 时的）
 * 写入；{@code ingestConfString} 是官方 Java 客户端的连接串，默认 QWP over WebSocket。
 * {@code writeMode} 为 {@code client} 时写入走 QWP，为 {@code jdbc} 时退回 JDBC batch。
 */
@ConfigurationProperties(prefix = "app.questdb")
public record QuestDbProperties(
        String jdbcUrl,
        String username,
        String password,
        int poolSize,
        long connectionTimeoutMs,
        String ingestConfString,
        String writeMode) {

    /**
     * 默认 QWP 连接串；lazy_connect 让 QuestDB 晚于应用启动也能启动。
     *
     * <p>sender pool 要覆盖所有并发 QWP 写入线程：8 个 funds-flow consumer + 8 个
     * order-history consumer + marketdata consumer（行情/K线/anchor），共 17 个。池子小于
     * 并发写入线程数时，多出的线程会等满 acquire_timeout（默认 10s）后抛
     * LineSenderException，导致消费停摆、lag 暴涨，所以默认留到 32。
     */
    private static final String DEFAULT_INGEST_CONF =
            "ws::addr=localhost:9000;lazy_connect=on;sender_pool_min=1;sender_pool_max=32;"
                    + "auto_flush_rows=75000;auto_flush_interval=1000;acquire_timeout_ms=10000;";

    public QuestDbProperties {
        if (ingestConfString == null || ingestConfString.isBlank()) {
            ingestConfString = DEFAULT_INGEST_CONF;
        }
        if (writeMode == null || writeMode.isBlank()) {
            writeMode = "client";
        }
    }

    /** 是否使用官方 Java 客户端（QWP）写入。 */
    public boolean clientWriteEnabled() {
        return "client".equalsIgnoreCase(writeMode.strip());
    }
}
