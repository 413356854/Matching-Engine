-- =============================================================================
-- 委托历史表（QuestDB）
--
-- 数据来源：Kafka topic me.order.history-batch（key=symbol，8 分区），
--           admin 模块消费后通过 QWP/JDBC 批量写入。
-- 写入范围：委托模块只收集进入终态的委托：
--           status = 'CANCELED'（撤单）/ 'FILLED'（撮合完成）。
-- 幂等：    final_at 是委托进入终态的时间，同一委托重投时保持不变；
--           按 (final_at, order_id) DEDUP，Kafka 事务回滚重投不会产生重复行。
-- 查询：    下单前端 GET /api/v1/orders/history
--           管理后台 GET /api/v1/admin/order-history
--
-- 应用启动后首次访问仓库时也会用同一 DDL 自动补建（QuestDbSchema）。本脚本用于
-- 手工初始化 / 迁移，可直接在 QuestDB Web Console 执行。
-- =============================================================================

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
DEDUP UPSERT KEYS(final_at, order_id);
