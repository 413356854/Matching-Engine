package com.me.admin.repository;

import com.me.admin.config.QuestDbIngestClient;
import com.me.admin.config.QuestDbSchema;
import com.me.admin.entity.FundsFlowRow;
import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.FundsOp;
import io.questdb.client.Sender;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 资金流水 QuestDB 访问层。
 *
 * <p>写入按 Kafka poll 批次走 PostgreSQL wire 的 JDBC batch（{@code batchUpdate}）；列表查询按
 * (user_id, currency) 走 QuestDB SYMBOL 索引，按 occurred_at 倒序。
 */
@Repository
public class FundsFlowRepository {

    private static final String INSERT_SQL = """
            INSERT INTO funds_flow
                (event_id, request_id, user_id, currency, log_type, op, log_desc, amount,
                 available_before, available_after, frozen_before, frozen_after, seq,
                 occurred_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final QuestDbIngestClient ingestClient;
    private volatile boolean tableReady;

    public FundsFlowRepository(@Qualifier("questDbJdbcTemplate") JdbcTemplate jdbcTemplate,
                               QuestDbIngestClient ingestClient) {
        this.jdbcTemplate = jdbcTemplate;
        this.ingestClient = ingestClient;
    }

    /**
     * 批量写入一个 Kafka poll 批次中的资金流水。
     *
     * <p>默认走官方 Java 客户端的 QWP（列式二进制、批量、等待服务端 ack）；配置
     * {@code app.questdb.write-mode=jdbc} 时退回 PostgreSQL wire 的 JDBC batch。
     *
     * <p>两种写入都不是原子操作：执行中途失败时前缀行可能已经写入。
     * {@code funds_flow} 没有 DEDUP，调用方只能在整批成功后才把 eventId 记入去重窗口，
     * 因此极端情况下重投可能重复写入该前缀。
     */
    public void insertBatch(List<FundsFlowEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        ensureTable();
        Instant archivedAt = Instant.now();
        if (ingestClient.enabled()) {
            ingestClient.writeBatch(sender -> events.forEach(
                    event -> append(sender, event, archivedAt)));
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                bind(ps, events.get(i), archivedAt);
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
    }

    /** 把一条流水追加到 QWP sender；{@code occurred_at} 是表指定时间戳，用 {@code at()} 写入。 */
    private static void append(Sender sender, FundsFlowEvent event, Instant archivedAt) {
        FundsLogType logType = event.logType();
        sender.table("funds_flow")
                .stringColumn("event_id", event.eventId())
                .stringColumn("request_id", event.requestId())
                .symbol("user_id", String.valueOf(event.userId()))
                .symbol("currency", event.currency())
                .symbol("log_type", logType.type())
                .symbol("op", logType.op().name())
                .stringColumn("log_desc", logType.description())
                .longColumn("amount", event.amount())
                .longColumn("available_before", event.availableBefore())
                .longColumn("available_after", event.availableAfter())
                .longColumn("frozen_before", event.frozenBefore())
                .longColumn("frozen_after", event.frozenAfter())
                .longColumn("seq", event.seq())
                .timestampColumn("archived_at", archivedAt)
                .at(event.occurredAt());
    }

    private static void bind(PreparedStatement ps, FundsFlowEvent event, Instant archivedAt)
            throws SQLException {
        FundsLogType logType = event.logType();
        ps.setString(1, event.eventId());
        ps.setString(2, event.requestId());
        ps.setString(3, String.valueOf(event.userId()));
        ps.setString(4, event.currency());
        ps.setString(5, logType.type());
        ps.setString(6, logType.op().name());
        ps.setString(7, logType.description());
        ps.setLong(8, event.amount());
        ps.setLong(9, event.availableBefore());
        ps.setLong(10, event.availableAfter());
        ps.setLong(11, event.frozenBefore());
        ps.setLong(12, event.frozenAfter());
        ps.setLong(13, event.seq());
        ps.setTimestamp(14, Timestamp.from(event.occurredAt()));
        ps.setTimestamp(15, Timestamp.from(archivedAt));
    }

    /**
     * 查询流水列表（按 {@code occurred_at} 倒序）。筛选条件均可选，全部为空时返回全表最近记录。
     *
     * @param userId   用户 ID；{@code null} 表示不筛选
     * @param currency 币种；{@code null} 表示不筛选（调用方保证已转大写）
     * @param logType  日志类型；{@code null} 表示不筛选
     * @param limit    1..1000，超出范围会被夹到边界值
     */
    public List<FundsFlowRow> findFlows(Long userId, String currency, FundsLogType logType,
                                        int limit) {
        ensureTable();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        StringBuilder sql = new StringBuilder("""
                SELECT event_id, request_id, user_id, currency, log_type, op, log_desc, amount,
                       available_before, available_after, frozen_before, frozen_after,
                       seq, occurred_at
                FROM funds_flow
                """);
        List<String> conditions = new ArrayList<>(3);
        List<Object> params = new ArrayList<>(3);
        if (userId != null) {
            conditions.add("user_id = ?");
            params.add(String.valueOf(userId));
        }
        if (currency != null) {
            conditions.add("currency = ?");
            params.add(currency);
        }
        if (logType != null) {
            conditions.add("log_type = ?");
            params.add(logType.type());
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(" ORDER BY occurred_at DESC LIMIT ").append(safeLimit);
        return jdbcTemplate.query(sql.toString(), FundsFlowRepository::mapRow, params.toArray());
    }

    /**
     * 一键删除全部流水，返回删除前的记录数。
     *
     * <p>QuestDB 不支持行级 {@code DELETE FROM}，因此整体清空使用 {@code TRUNCATE TABLE}；
     * 表结构与索引保留，之后新产生的流水仍可正常写入。
     */
    public long truncateAll() {
        ensureTable();
        Long count = jdbcTemplate.queryForObject("SELECT count() FROM funds_flow", Long.class);
        jdbcTemplate.execute("TRUNCATE TABLE funds_flow");
        return count == null ? 0 : count;
    }

    private static FundsFlowRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new FundsFlowRow(
                rs.getString("event_id"),
                rs.getString("request_id"),
                Long.parseLong(rs.getString("user_id")),
                rs.getString("currency"),
                FundsLogType.valueOf(rs.getString("log_type")),
                FundsOp.valueOf(rs.getString("op")),
                rs.getString("log_desc"),
                rs.getLong("amount"),
                rs.getLong("available_before"),
                rs.getLong("available_after"),
                rs.getLong("frozen_before"),
                rs.getLong("frozen_after"),
                rs.getLong("seq"),
                rs.getTimestamp("occurred_at").toInstant());
    }

    /**
     * QuestDB 启动晚于应用时，首次实际访问前补建表；失败则抛出，由 Kafka 监听容器重试。
     */
    private void ensureTable() {
        if (!tableReady) {
            jdbcTemplate.execute(QuestDbSchema.CREATE_FUNDS_FLOW_TABLE);
            tableReady = true;
        }
    }
}
