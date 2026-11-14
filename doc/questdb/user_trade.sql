-- =============================================================================
-- 用户成交记录表（QuestDB）
--
-- 数据来源：Kafka topic me.match-pair-batch（key=symbol，8 分区），
--           admin 模块以消费组 admin-user-trade 消费，通过 QWP/JDBC 批量写入。
-- 写入范围：每个撮合对拆成 taker / maker 两条记录，同一笔成交对每个用户各一行：
--           role = 'TAKER' 行记录主动方用户与其委托；
--           role = 'MAKER' 行记录被动方用户与其委托；
--           side 是当前用户这一侧的方向（maker 与 taker 相反）。
-- 幂等：    executed_at 是成交时间，同一撮合对重投时保持不变；
--           按 (executed_at, trade_id, order_id) DEDUP，
--           Kafka 事务回滚重投不会产生重复行。
-- 关联：    trade_id 与行情模块 md_trade.trade_id 对应；
--           order_id 与 ord_history.order_id 对应。
--
-- 应用启动后首次访问仓库时也会用同一 DDL 自动补建（QuestDbSchema）。本脚本用于
-- 手工初始化 / 迁移，可直接在 QuestDB Web Console 执行。
-- =============================================================================

CREATE TABLE IF NOT EXISTS user_trade (
    trade_id LONG,
    symbol SYMBOL INDEX CAPACITY 1024,
    user_id SYMBOL INDEX CAPACITY 65536,
    order_id LONG,
    role SYMBOL,
    counter_order_id LONG,
    side SYMBOL,
    price LONG,
    quantity LONG,
    amount LONG,
    executed_at TIMESTAMP,
    archived_at TIMESTAMP
) TIMESTAMP(executed_at) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(executed_at, trade_id, order_id);
