package com.me.admin.repository;

import com.me.admin.config.QuestDbIngestClient;
import com.me.admin.config.QuestDbSchema;
import com.me.admin.entity.OrderHistoryRow;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import io.questdb.client.Sender;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 委托历史 QuestDB 访问层。
 *
 * <p>写入按 Kafka poll 批次走 QWP（配置 {@code app.questdb.write-mode=jdbc} 时退回 JDBC batch），
 * 表按 {@code (final_at, order_id)} DEDUP：同一委托的历史事件重投不会产生重复行。
 * 查询按 {@code final_at} 倒序，筛选条件（用户 / 交易对 / 状态）均可选。
 */
@Repository
public class OrderHistoryRepository {

    private static final String INSERT_SQL = """
            INSERT INTO ord_history
                (order_id, request_id, user_id, symbol, side, type, status, price, quantity,
                 filled_quantity, frozen_currency, frozen_amount, price_scale, quantity_scale,
                 quote_scale, seq, created_at, final_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final QuestDbIngestClient ingestClient;
    private volatile boolean tableReady;

    public OrderHistoryRepository(@Qualifier("questDbJdbcTemplate") JdbcTemplate jdbcTemplate,
                                  QuestDbIngestClient ingestClient) {
        this.jdbcTemplate = jdbcTemplate;
        this.ingestClient = ingestClient;
    }

    /** 批量写入一个 Kafka poll 批次中的委托历史；DEDUP 键保证消费重投不会重复。 */
    public void insertBatch(List<OrderHistoryEvent> events) {
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

    private static void append(Sender sender, OrderHistoryEvent event, Instant archivedAt) {
        sender.table("ord_history")
                .longColumn("order_id", event.orderId())
                .stringColumn("request_id", event.requestId())
                .symbol("user_id", String.valueOf(event.userId()))
                .symbol("symbol", event.symbol())
                .symbol("side", event.side().name())
                .symbol("type", event.type().name())
                .symbol("status", event.status().name())
                .longColumn("price", event.price())
                .longColumn("quantity", event.quantity())
                .longColumn("filled_quantity", event.filledQuantity())
                .symbol("frozen_currency", event.frozenCurrency())
                .longColumn("frozen_amount", event.frozenAmount())
                .intColumn("price_scale", event.priceScale())
                .intColumn("quantity_scale", event.quantityScale())
                .intColumn("quote_scale", event.quoteScale())
                .longColumn("seq", event.seq())
                .timestampColumn("created_at", event.createdAt())
                .timestampColumn("archived_at", archivedAt)
                .at(event.finalAt());
    }

    private static void bind(PreparedStatement ps, OrderHistoryEvent event, Instant archivedAt)
            throws SQLException {
        ps.setLong(1, event.orderId());
        ps.setString(2, event.requestId());
        ps.setString(3, String.valueOf(event.userId()));
        ps.setString(4, event.symbol());
        ps.setString(5, event.side().name());
        ps.setString(6, event.type().name());
        ps.setString(7, event.status().name());
        ps.setLong(8, event.price());
        ps.setLong(9, event.quantity());
        ps.setLong(10, event.filledQuantity());
        ps.setString(11, event.frozenCurrency());
        ps.setLong(12, event.frozenAmount());
        ps.setInt(13, event.priceScale());
        ps.setInt(14, event.quantityScale());
        ps.setInt(15, event.quoteScale());
        ps.setLong(16, event.seq());
        ps.setTimestamp(17, Timestamp.from(event.createdAt()));
        ps.setTimestamp(18, Timestamp.from(event.finalAt()));
        ps.setTimestamp(19, Timestamp.from(archivedAt));
    }

    /**
     * 查询委托历史，按最终时间倒序（最新在前）。
     *
     * @param userId 用户 ID；{@code null} 表示不筛选
     * @param symbol 交易对（大写）；{@code null} 表示不筛选
     * @param status 终态（CANCELED / FILLED）；{@code null} 表示不筛选
     * @param limit  1..1000，超出范围会被夹到边界值
     */
    public List<OrderHistoryRow> findHistory(Long userId, String symbol, OrderStatus status,
                                             int limit) {
        ensureTable();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        StringBuilder sql = new StringBuilder("""
                SELECT order_id, request_id, user_id, symbol, side, type, status, price, quantity,
                       filled_quantity, frozen_currency, frozen_amount, price_scale,
                       quantity_scale, quote_scale, seq, created_at, final_at
                FROM ord_history
                """);
        List<String> conditions = new ArrayList<>(3);
        List<Object> params = new ArrayList<>(3);
        if (userId != null) {
            conditions.add("user_id = ?");
            params.add(String.valueOf(userId));
        }
        if (symbol != null) {
            conditions.add("symbol = ?");
            params.add(symbol);
        }
        if (status != null) {
            conditions.add("status = ?");
            params.add(status.name());
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(" ORDER BY final_at DESC, order_id DESC LIMIT ").append(safeLimit);
        return jdbcTemplate.query(sql.toString(), OrderHistoryRepository::mapRow,
                params.toArray());
    }

    private static OrderHistoryRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new OrderHistoryRow(
                rs.getLong("order_id"),
                rs.getString("request_id"),
                Long.parseLong(rs.getString("user_id")),
                rs.getString("symbol"),
                OrderSide.valueOf(rs.getString("side")),
                OrderType.valueOf(rs.getString("type")),
                OrderStatus.valueOf(rs.getString("status")),
                rs.getLong("price"),
                rs.getLong("quantity"),
                rs.getLong("filled_quantity"),
                rs.getString("frozen_currency"),
                rs.getLong("frozen_amount"),
                rs.getInt("price_scale"),
                rs.getInt("quantity_scale"),
                rs.getInt("quote_scale"),
                rs.getLong("seq"),
                readInstant(rs, "created_at"),
                readInstant(rs, "final_at"));
    }

    /**
     * QuestDB 的 TIMESTAMP 语义是 UTC，但 pgwire 返回的是不带时区的值，PostgreSQL JDBC
     * 驱动会按 JVM 默认时区解释。显式用 UTC Calendar 读取，避免非 UTC 环境下查询结果偏移。
     */
    private static Instant readInstant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column, Calendar.getInstance(TimeZone.getTimeZone("UTC")))
                .toInstant();
    }

    /**
     * QuestDB 启动晚于应用时，首次实际访问前补建表；失败则抛出，由 Kafka 监听容器重试。
     */
    private void ensureTable() {
        if (!tableReady) {
            jdbcTemplate.execute(QuestDbSchema.CREATE_ORDER_HISTORY_TABLE);
            tableReady = true;
        }
    }
}
