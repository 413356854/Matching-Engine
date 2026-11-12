-- =====================================================================
-- 币币模块 · 交易对表 pair_symbol
-- 库：db_exchange（MySQL 8.0.17，utf8mb4 / utf8mb4_0900_ai_ci）
-- 说明：
--   1. 价格/数量在撮合与账本里一律是最小单位的整数，本表只定义精度与限额；
--   2. 费率单位是基点（bps）：1 bps = 0.01%，10000 bps = 100%；
--      允许负值表示返佣（rebate）；
--   3. 脚本幂等，可重复执行。
-- =====================================================================

CREATE TABLE IF NOT EXISTS `pair_symbol` (
    `id`                 BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '主键',
    `symbol`             VARCHAR(32)      NOT NULL COMMENT '交易对代码（大写），如 BTC-USDT',
    `base_currency`      VARCHAR(32)      NOT NULL COMMENT '基础币种代码（被买卖的币），如 BTC',
    `quote_currency`     VARCHAR(32)      NOT NULL COMMENT '计价币种代码，如 USDT',
    `price_scale`        TINYINT UNSIGNED NOT NULL DEFAULT 2 COMMENT '价格小数位数',
    `quantity_scale`     TINYINT UNSIGNED NOT NULL DEFAULT 8 COMMENT '数量小数位数',
    `min_order_quantity` BIGINT           NOT NULL DEFAULT 0 COMMENT '最小下单量，基础币最小单位',
    `min_order_amount`   BIGINT           NOT NULL DEFAULT 0 COMMENT '最小下单金额，计价币最小单位',
    `maker_fee_bps`      INT              NOT NULL DEFAULT 0 COMMENT 'maker 费率，基点（1 bps = 0.01%）',
    `taker_fee_bps`      INT              NOT NULL DEFAULT 0 COMMENT 'taker 费率，基点（1 bps = 0.01%）',
    `status`             TINYINT          NOT NULL DEFAULT 1 COMMENT '状态：1=上线，0=下线',
    `sort_order`         INT              NOT NULL DEFAULT 0 COMMENT '展示排序，升序',
    `created_at`         DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_at`         DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                          ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pair_symbol_symbol` (`symbol`),
    KEY `idx_pair_symbol_base_quote` (`base_currency`, `quote_currency`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '币币模块-交易对';
