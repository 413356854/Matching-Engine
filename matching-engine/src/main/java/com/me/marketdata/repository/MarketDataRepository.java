package com.me.marketdata.repository;

import com.me.admin.config.QuestDbIngestClient;
import com.me.marketdata.config.MarketDataSchema;
import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineAnchor;
import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.entity.TradeRecord;
import com.me.shared.event.MarketDataEvent;
import com.me.shared.order.OrderSide;
import io.questdb.client.Sender;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 行情模块 QuestDB 访问层：成交明细 {@code md_trade}、K 线 {@code md_kline} 与锚点
 * {@code md_kline_anchor}。
 *
 * <p>复用 admin 模块建立的 QuestDB 独立数据源（PostgreSQL wire，8812），通过 bean 名注入，
 * 不直接依赖 admin 的类。写入按 Kafka poll 批次走 JDBC batch（{@code batchUpdate}）；
 * 三张表都用 DEDUP 做重投幂等，锚点表以固定时间戳作 DEDUP 键实现按
 * {@code (symbol, interval)} 覆盖写。
 */
@Repository
public class MarketDataRepository {

    private static final String INSERT_TRADE_SQL = """
            INSERT INTO md_trade
                (trade_id, symbol, taker_order_id, maker_order_id, price, quantity, amount,
                 taker_side, executed_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_KLINE_SQL = """
            INSERT INTO md_kline
                (symbol, bar_interval, bucket_start, open_price, high_price, low_price,
                 close_price, volume, amount, trade_count, source_partition, source_offset,
                 updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /**
     * 查询最近 N 条 K 线。内层按 bucket 取 {@code updated_at} 最新的一条，避免 DEDUP 尚未
     * apply 时同一 bucket 返回多行；外层先倒序取最近 N 个 bucket，再由调用方翻转为升序。
     */
    private static final String SELECT_KLINES_SQL = """
            SELECT symbol, bar_interval, bucket_start, open_price, high_price, low_price,
                   close_price, volume, amount, trade_count, source_partition, source_offset
            FROM (
                SELECT *, row_number() OVER (PARTITION BY bucket_start ORDER BY updated_at DESC) AS rn
                FROM md_kline
                WHERE symbol = ? AND bar_interval = ?
            )
            WHERE rn = 1
            ORDER BY bucket_start DESC
            LIMIT %d
            """;

    /**
     * 读取 {@code (symbol, interval)} 的锚点。锚点表对同一 key 是覆盖写，DEDUP 生效后只有一行；
     * 这里按 {@code updated_at} 倒序取第一条，避免 DEDUP 尚未 apply 时读到旧锚点。
     */
    private static final String SELECT_ANCHOR_SQL = """
            SELECT symbol, bar_interval, bucket_start
            FROM md_kline_anchor
            WHERE symbol = ? AND bar_interval = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """;

    /**
     * 写入锚点。{@code anchor_key} 固定 epoch 0，配合 DEDUP 键
     * {@code (anchor_key, symbol, bar_interval)} 实现按 {@code (symbol, interval)} 覆盖。
     */
    private static final String INSERT_ANCHOR_SQL = """
            INSERT INTO md_kline_anchor
                (symbol, bar_interval, bucket_start, updated_at, anchor_key)
            VALUES (?, ?, ?, ?, ?)
            """;

    /**
     * 查询 {@code [from, to]} 闭区间内已有的 K 线，按 bucket 升序；同一 bucket 先取
     * {@code updated_at} 最新的一行（DEDUP 尚未 apply 时可能短暂出现多行）。
     */
    private static final String SELECT_KLINES_BETWEEN_SQL = """
            SELECT symbol, bar_interval, bucket_start, open_price, high_price, low_price,
                   close_price, volume, amount, trade_count, source_partition, source_offset
            FROM md_kline
            WHERE symbol = ? AND bar_interval = ? AND bucket_start BETWEEN ? AND ?
            ORDER BY bucket_start ASC, updated_at DESC
            """;

    /**
     * 查询指定 {@code (symbol, interval)} 最新的一根 K 线；没有则返回 {@code null}。
     *
     * <p>锚点缺失（锚点表刚上线 / 锚点被清空）时用它充当“上一根”，恢复后由调用方补齐
     * 到事件 bucket 之间的空柱，因此不能只按事件 bucket 精确查找。
     */
    private static final String SELECT_LATEST_KLINE_SQL = """
            SELECT symbol, bar_interval, bucket_start, open_price, high_price, low_price,
                   close_price, volume, amount, trade_count, source_partition, source_offset
            FROM md_kline
            WHERE symbol = ? AND bar_interval = ?
            ORDER BY bucket_start DESC, updated_at DESC
            LIMIT 1
            """;

    /** 查询最近 N 笔成交，最新在前。 */
    private static final String SELECT_TRADES_SQL = """
            SELECT trade_id, symbol, price, quantity, amount, taker_side, executed_at
            FROM md_trade
            WHERE symbol = ?
            ORDER BY executed_at DESC, trade_id DESC
            LIMIT %d
            """;

    private final JdbcTemplate jdbcTemplate;
    private final QuestDbIngestClient ingestClient;
    private volatile boolean tablesReady;

    public MarketDataRepository(@Qualifier("questDbJdbcTemplate") JdbcTemplate jdbcTemplate,
                                QuestDbIngestClient ingestClient) {
        this.jdbcTemplate = jdbcTemplate;
        this.ingestClient = ingestClient;
    }

    /** 批量写入一个 Kafka poll 批次中的成交；DEDUP 键保证消费重投不会产生重复行。 */
    public void insertTrades(List<MarketDataEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        ensureTables();
        Instant archivedAt = Instant.now();
        if (ingestClient.enabled()) {
            ingestClient.writeBatch(sender -> events.forEach(
                    event -> appendTrade(sender, event, archivedAt)));
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_TRADE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                bindTrade(ps, events.get(i), archivedAt);
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
    }

    /** 批量写入 K 线快照；DEDUP 按 (bucket_start, symbol, bar_interval) 保留最新一条。 */
    public void insertKlines(List<Kline> klines) {
        if (klines.isEmpty()) {
            return;
        }
        ensureTables();
        Instant updatedAt = Instant.now();
        if (ingestClient.enabled()) {
            ingestClient.writeBatch(sender -> klines.forEach(
                    kline -> appendKline(sender, kline, updatedAt)));
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_KLINE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                bindKline(ps, klines.get(i), updatedAt);
            }

            @Override
            public int getBatchSize() {
                return klines.size();
            }
        });
    }

    /**
     * 批量写入锚点；同一 {@code (symbol, interval)} 后写覆盖，表里始终只有一行。
     * 调用方保证在 K 线写入成功之后再写锚点。
     */
    public void upsertAnchors(List<KlineAnchor> anchors) {
        if (anchors.isEmpty()) {
            return;
        }
        ensureTables();
        Instant updatedAt = Instant.now();
        if (ingestClient.enabled()) {
            ingestClient.writeBatch(sender -> anchors.forEach(
                    anchor -> appendAnchor(sender, anchor, updatedAt)));
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_ANCHOR_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                KlineAnchor anchor = anchors.get(i);
                ps.setString(1, anchor.symbol());
                ps.setString(2, anchor.interval().label());
                ps.setTimestamp(3, Timestamp.from(anchor.bucketStart()));
                ps.setTimestamp(4, Timestamp.from(updatedAt));
                ps.setTimestamp(5, Timestamp.from(Instant.EPOCH));
            }

            @Override
            public int getBatchSize() {
                return anchors.size();
            }
        });
    }

    private static void bindTrade(PreparedStatement ps, MarketDataEvent event, Instant archivedAt)
            throws SQLException {
        ps.setLong(1, event.tradeId());
        ps.setString(2, event.symbol());
        ps.setLong(3, event.takerOrderId());
        ps.setLong(4, event.makerOrderId());
        ps.setLong(5, event.price());
        ps.setLong(6, event.quantity());
        ps.setLong(7, event.amount());
        ps.setString(8, event.takerSide() == null ? null : event.takerSide().name());
        ps.setTimestamp(9, Timestamp.from(event.executedAt()));
        ps.setTimestamp(10, Timestamp.from(archivedAt));
    }

    private static void bindKline(PreparedStatement ps, Kline kline, Instant updatedAt)
            throws SQLException {
        ps.setString(1, kline.symbol());
        ps.setString(2, kline.interval().label());
        ps.setTimestamp(3, Timestamp.from(kline.bucketStart()));
        ps.setLong(4, kline.openPrice());
        ps.setLong(5, kline.highPrice());
        ps.setLong(6, kline.lowPrice());
        ps.setLong(7, kline.closePrice());
        ps.setLong(8, kline.volume());
        ps.setLong(9, kline.amount());
        ps.setLong(10, kline.tradeCount());
        ps.setInt(11, kline.sourcePartition());
        ps.setLong(12, kline.sourceOffset());
        ps.setTimestamp(13, Timestamp.from(updatedAt));
    }

    /** 把一条成交追加到 QWP sender；{@code executed_at} 是表指定时间戳。 */
    private static void appendTrade(Sender sender, MarketDataEvent event, Instant archivedAt) {
        Sender row = sender.table("md_trade")
                .longColumn("trade_id", event.tradeId())
                .symbol("symbol", event.symbol())
                .longColumn("taker_order_id", event.takerOrderId())
                .longColumn("maker_order_id", event.makerOrderId())
                .longColumn("price", event.price())
                .longColumn("quantity", event.quantity())
                .longColumn("amount", event.amount());
        if (event.takerSide() != null) {
            row.symbol("taker_side", event.takerSide().name());
        }
        row.timestampColumn("archived_at", archivedAt)
                .at(event.executedAt());
    }

    /** 把一条 K 线快照追加到 QWP sender；{@code bucket_start} 是表指定时间戳。 */
    private static void appendKline(Sender sender, Kline kline, Instant updatedAt) {
        sender.table("md_kline")
                .symbol("symbol", kline.symbol())
                .symbol("bar_interval", kline.interval().label())
                .longColumn("open_price", kline.openPrice())
                .longColumn("high_price", kline.highPrice())
                .longColumn("low_price", kline.lowPrice())
                .longColumn("close_price", kline.closePrice())
                .longColumn("volume", kline.volume())
                .longColumn("amount", kline.amount())
                .longColumn("trade_count", kline.tradeCount())
                .intColumn("source_partition", kline.sourcePartition())
                .longColumn("source_offset", kline.sourceOffset())
                .timestampColumn("updated_at", updatedAt)
                .at(kline.bucketStart());
    }

    /**
     * 把一条锚点追加到 QWP sender。{@code anchor_key} 固定 epoch 0（表指定时间戳），
     * 配合 DEDUP 键实现按 {@code (symbol, interval)} 覆盖。
     */
    private static void appendAnchor(Sender sender, KlineAnchor anchor, Instant updatedAt) {
        sender.table("md_kline_anchor")
                .symbol("symbol", anchor.symbol())
                .symbol("bar_interval", anchor.interval().label())
                .timestampColumn("bucket_start", anchor.bucketStart())
                .timestampColumn("updated_at", updatedAt)
                .at(Instant.EPOCH);
    }

    /** 读取 {@code (symbol, interval)} 的锚点；没有则返回 {@code null}。 */
    public KlineAnchor findAnchor(String symbol, KlineInterval interval) {
        ensureTables();
        List<KlineAnchor> rows = jdbcTemplate.query(SELECT_ANCHOR_SQL,
                (rs, rowNum) -> new KlineAnchor(
                        rs.getString("symbol"),
                        KlineInterval.of(rs.getString("bar_interval")),
                        readInstant(rs, "bucket_start")),
                symbol, interval.label());
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * 读取 {@code [from, to]} 闭区间内已有的 K 线，按 bucket 升序返回。
     *
     * <p>同一 bucket 可能因 DEDUP 尚未 apply 出现多行，{@code updated_at} 最新的排在最前，
     * 调用方用 {@code putIfAbsent} 保留第一行即可。
     */
    public List<Kline> findKlinesBetween(String symbol, KlineInterval interval,
                                         Instant from, Instant to) {
        ensureTables();
        return jdbcTemplate.query(SELECT_KLINES_BETWEEN_SQL, MarketDataRepository::mapKline,
                symbol, interval.label(), Timestamp.from(from), Timestamp.from(to));
    }

    /**
     * 读取指定 {@code (symbol, interval)} 最新的一根 K 线；没有则返回 {@code null}。
     *
     * <p>锚点缺失时用它恢复聚合位置，调用方据此补齐它到事件 bucket 之间的空柱。
     */
    public Kline findLatestKline(String symbol, KlineInterval interval) {
        ensureTables();
        List<Kline> rows = jdbcTemplate.query(SELECT_LATEST_KLINE_SQL,
                MarketDataRepository::mapKline, symbol, interval.label());
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * 查询最近 {@code limit} 条 K 线，按 bucket 起点升序返回（便于直接画图）。
     *
     * @param limit 1..1000，超出范围会被夹到边界值
     */
    public List<Kline> findKlines(String symbol, KlineInterval interval, int limit) {
        ensureTables();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        List<Kline> rows = jdbcTemplate.query(SELECT_KLINES_SQL.formatted(safeLimit),
                MarketDataRepository::mapKline,
                symbol, interval.label());
        Collections.reverse(rows);
        return rows;
    }

    /**
     * 管理端查询最近 {@code limit} 条 K 线，按 bucket 起点倒序（最新在前）。
     *
     * <p>{@code symbol} / {@code interval} 均可选，全部为空时返回全表最近记录。内层按
     * {@code (symbol, bar_interval, bucket_start)} 取 {@code updated_at} 最新的一行，
     * 避免 WAL DEDUP 尚未 apply 时同一条 K 线返回多行。
     *
     * @param limit 1..1000，超出范围会被夹到边界值
     */
    public List<Kline> findRecentKlines(String symbol, KlineInterval interval, int limit) {
        ensureTables();
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        StringBuilder sql = new StringBuilder("""
                SELECT symbol, bar_interval, bucket_start, open_price, high_price, low_price,
                       close_price, volume, amount, trade_count, source_partition, source_offset
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY symbol, bar_interval, bucket_start
                        ORDER BY updated_at DESC) AS rn
                    FROM md_kline
                """);
        List<String> conditions = new ArrayList<>(2);
        List<Object> params = new ArrayList<>(2);
        if (symbol != null) {
            conditions.add("symbol = ?");
            params.add(symbol);
        }
        if (interval != null) {
            conditions.add("bar_interval = ?");
            params.add(interval.label());
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sql.append(") WHERE rn = 1 ORDER BY bucket_start DESC LIMIT ").append(safeLimit);
        return jdbcTemplate.query(sql.toString(), MarketDataRepository::mapKline, params.toArray());
    }

    /**
     * 一键删除全部 K 线与锚点，返回删除前的 K 线记录数。
     *
     * <p>QuestDB 不支持行级 {@code DELETE FROM}，因此整体清空使用 {@code TRUNCATE TABLE}。
     * 锚点必须一起清空，否则下次成交会从旧锚点续算；成交明细 {@code md_trade} 不受影响。
     */
    public long truncateKlines() {
        ensureTables();
        Long count = jdbcTemplate.queryForObject("SELECT count() FROM md_kline", Long.class);
        jdbcTemplate.execute("TRUNCATE TABLE md_kline");
        jdbcTemplate.execute("TRUNCATE TABLE md_kline_anchor");
        return count == null ? 0 : count;
    }

    /**
     * 查询最近 {@code limit} 笔成交，按成交时间倒序返回（最新在前）。
     *
     * @param limit 1..500，超出范围会被夹到边界值
     */
    public List<TradeRecord> findTrades(String symbol, int limit) {
        ensureTables();
        int safeLimit = Math.max(1, Math.min(limit, 500));
        return jdbcTemplate.query(SELECT_TRADES_SQL.formatted(safeLimit),
                MarketDataRepository::mapTrade,
                symbol);
    }

    /**
     * QuestDB 的 TIMESTAMP 语义是 UTC，但 pgwire 返回的是不带时区的值，PostgreSQL JDBC 驱动会按
     * JVM 默认时区解释。显式用 UTC Calendar 读取，否则在非 UTC 环境下 {@code toInstant()}
     * 会整体偏移，导致锚点 / bucket 比较错位。
     */
    private static Instant readInstant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column, Calendar.getInstance(TimeZone.getTimeZone("UTC")))
                .toInstant();
    }

    private static Kline mapKline(ResultSet rs, int rowNum) throws SQLException {
        return new Kline(
                rs.getString("symbol"),
                KlineInterval.of(rs.getString("bar_interval")),
                readInstant(rs, "bucket_start"),
                rs.getLong("open_price"),
                rs.getLong("high_price"),
                rs.getLong("low_price"),
                rs.getLong("close_price"),
                rs.getLong("volume"),
                rs.getLong("amount"),
                rs.getLong("trade_count"),
                rs.getInt("source_partition"),
                rs.getLong("source_offset"));
    }

    private static TradeRecord mapTrade(ResultSet rs, int rowNum) throws SQLException {
        String takerSide = rs.getString("taker_side");
        return new TradeRecord(
                rs.getLong("trade_id"),
                rs.getString("symbol"),
                rs.getLong("price"),
                rs.getLong("quantity"),
                rs.getLong("amount"),
                takerSide == null ? null : OrderSide.valueOf(takerSide),
                readInstant(rs, "executed_at"));
    }

    /**
     * QuestDB 启动晚于应用时，首次实际访问前补建表；失败则抛出，由 Kafka 监听容器重试。
     */
    private void ensureTables() {
        if (!tablesReady) {
            jdbcTemplate.execute(MarketDataSchema.CREATE_MD_TRADE_TABLE);
            jdbcTemplate.execute(MarketDataSchema.CREATE_MD_KLINE_TABLE);
            jdbcTemplate.execute(MarketDataSchema.CREATE_MD_KLINE_ANCHOR_TABLE);
            tablesReady = true;
        }
    }
}
