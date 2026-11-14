package com.me.admin.repository;

import com.me.admin.config.QuestDbIngestClient;
import com.me.admin.config.QuestDbSchema;
import com.me.admin.entity.UserTradeRow;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.trade.UserTradeRole;
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
 * 用户成交记录 QuestDB 访问层。
 *
 * <p>每个撮合对拆成 taker / maker 两条记录：同一笔成交对每个用户各有一行，包含该用户自己的
 * {@code order_id}、方向、对手单号与成交价格 / 数量 / 金额。表按
 * {@code (executed_at, trade_id, order_id)} DEDUP，Kafka 事务回滚重投不会产生重复行。
 *
 * <p>写入按 Kafka poll 批次走 QWP（配置 {@code app.questdb.write-mode=jdbc} 时退回 JDBC
 * batch）；{@code user_id} / {@code symbol} 建 SYMBOL 索引，支撑按用户 / 交易对查询。
 */
@Repository
public class UserTradeRepository {

    private static final String ROLE_TAKER = "TAKER";
    private static final String ROLE_MAKER = "MAKER";

    private static final String INSERT_SQL = """
            INSERT INTO user_trade
                (trade_id, symbol, user_id, order_id, role, counter_order_id, side, price,
                 quantity, amount, executed_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_SQL = """
            SELECT trade_id, symbol, user_id, order_id, role, counter_order_id, side,
                   price, quantity, amount, executed_at
            FROM user_trade
            """;

    private final JdbcTemplate jdbcTemplate;
    private final QuestDbIngestClient ingestClient;
    private volatile boolean tableReady;

    public UserTradeRepository(@Qualifier("questDbJdbcTemplate") JdbcTemplate jdbcTemplate,
                               QuestDbIngestClient ingestClient) {
        this.jdbcTemplate = jdbcTemplate;
        this.ingestClient = ingestClient;
    }

    /** 批量写入一个 Kafka poll 批次中的撮合对；每个撮合对写 taker / maker 两行。 */
    public void insertBatch(List<MatchPairEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        ensureTable();
        Instant archivedAt = Instant.now();
        if (ingestClient.enabled()) {
            ingestClient.writeBatch(sender -> events.forEach(event -> {
                appendLeg(sender, event, true, archivedAt);
                appendLeg(sender, event, false, archivedAt);
            }));
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                bindLeg(ps, events.get(i / 2), (i & 1) == 0, archivedAt);
            }

            @Override
            public int getBatchSize() {
                return events.size() * 2;
            }
        });
    }

    /** 追加一条用户成交到 QWP sender；{@code executed_at} 是表指定时间戳。 */
    private static void appendLeg(Sender sender, MatchPairEvent event, boolean taker,
                                  Instant archivedAt) {
        long userId = taker ? event.takerUserId() : event.makerUserId();
        long orderId = taker ? event.takerOrderId() : event.makerOrderId();
        long counterOrderId = taker ? event.makerOrderId() : event.takerOrderId();
        OrderSide side = taker ? event.takerSide() : opposite(event.takerSide());
        Sender row = sender.table("user_trade")
                .longColumn("trade_id", event.tradeId())
                .symbol("symbol", event.symbol())
                .symbol("user_id", String.valueOf(userId))
                .longColumn("order_id", orderId)
                .symbol("role", taker ? ROLE_TAKER : ROLE_MAKER)
                .longColumn("counter_order_id", counterOrderId)
                .longColumn("price", event.price())
                .longColumn("quantity", event.quantity())
                .longColumn("amount", event.amount());
        if (side != null) {
            row.symbol("side", side.name());
        }
        row.timestampColumn("archived_at", archivedAt)
                .at(event.executedAt());
    }

    private static void bindLeg(PreparedStatement ps, MatchPairEvent event, boolean taker,
                                Instant archivedAt) throws SQLException {
        long userId = taker ? event.takerUserId() : event.makerUserId();
        long orderId = taker ? event.takerOrderId() : event.makerOrderId();
        long counterOrderId = taker ? event.makerOrderId() : event.takerOrderId();
        OrderSide side = taker ? event.takerSide() : opposite(event.takerSide());
        ps.setLong(1, event.tradeId());
        ps.setString(2, event.symbol());
        ps.setString(3, String.valueOf(userId));
        ps.setLong(4, orderId);
        ps.setString(5, taker ? ROLE_TAKER : ROLE_MAKER);
        ps.setLong(6, counterOrderId);
        ps.setString(7, side == null ? null : side.name());
        ps.setLong(8, event.price());
        ps.setLong(9, event.quantity());
        ps.setLong(10, event.amount());
        ps.setTimestamp(11, Timestamp.from(event.executedAt()));
        ps.setTimestamp(12, Timestamp.from(archivedAt));
    }

    /** maker 的方向与 taker 相反。 */
    private static OrderSide opposite(OrderSide side) {
        if (side == null) {
            return null;
        }
        return side == OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY;
    }

    /**
     * 查询用户成交记录，按成交时间倒序（最新在前）。筛选条件均可选。
     *
     * @param userId 用户 ID；{@code null} 表示不筛选
     * @param symbol 交易对（大写）；{@code null} 表示不筛选
     * @param role   TAKER / MAKER；{@code null} 表示不筛选
     * @param limit  1..1000，超出范围会被夹到边界值
     */
    public List<UserTradeRow> findTrades(Long userId, String symbol, UserTradeRole role,
                                         int limit) {
        ensureTable();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        StringBuilder sql = new StringBuilder(SELECT_SQL);
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
        if (role != null) {
            conditions.add("role = ?");
            params.add(role.name());
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(" ORDER BY executed_at DESC, trade_id DESC, order_id DESC LIMIT ")
                .append(safeLimit);
        return jdbcTemplate.query(sql.toString(), UserTradeRepository::mapRow, params.toArray());
    }

    private static UserTradeRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        String side = rs.getString("side");
        return new UserTradeRow(
                rs.getLong("trade_id"),
                rs.getString("symbol"),
                Long.parseLong(rs.getString("user_id")),
                rs.getLong("order_id"),
                UserTradeRole.valueOf(rs.getString("role")),
                rs.getLong("counter_order_id"),
                side == null ? null : OrderSide.valueOf(side),
                rs.getLong("price"),
                rs.getLong("quantity"),
                rs.getLong("amount"),
                readInstant(rs, "executed_at"));
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
            jdbcTemplate.execute(QuestDbSchema.CREATE_USER_TRADE_TABLE);
            tableReady = true;
        }
    }
}
