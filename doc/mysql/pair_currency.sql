-- =====================================================================
-- 币币模块 · 币种表 pair_currency
-- 库：db_exchange（MySQL 8.0.17，utf8mb4 / utf8mb4_0900_ai_ci）
-- 说明：
--   1. 金额/数量一律用最小单位整数，不用浮点或 DECIMAL；
--   2. scale 表示小数位数，最小单位 = 10^-scale（BTC scale=8 → 1 BTC = 10^8 sat）；
--   3. 脚本幂等，可重复执行。
-- =====================================================================

CREATE TABLE IF NOT EXISTS `pair_currency` (
    `id`         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '主键',
    `code`       VARCHAR(32)      NOT NULL COMMENT '币种代码（大写），如 BTC、USDT',
    `name`       VARCHAR(64)      NOT NULL COMMENT '币种名称，如 比特币',
    `scale`      TINYINT UNSIGNED NOT NULL DEFAULT 8 COMMENT '小数位数，最小单位 = 10^-scale',
    `status`     TINYINT          NOT NULL DEFAULT 1 COMMENT '状态：1=启用，0=停用',
    `sort_order` INT              NOT NULL DEFAULT 0 COMMENT '展示排序，升序',
    `created_at` DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_at` DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                  ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pair_currency_code` (`code`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '币币模块-币种';
