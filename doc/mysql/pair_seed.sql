-- =====================================================================
-- 币币模块 · 演示数据（幂等，可重复执行）
-- 库：db_exchange
-- 说明：
--   1. 先执行 pair_currency.sql / pair_symbol.sql 建表；
--   2. 金额/数量一律是最小单位整数，price_scale / quantity_scale 只影响展示；
--   3. 下单接口会校验交易对存在且 status=1，最小下单量/金额为 0 表示不限制。
-- =====================================================================

INSERT INTO `pair_currency` (`code`, `name`, `scale`, `status`, `sort_order`)
VALUES ('BTC', '比特币', 8, 1, 1),
       ('ETH', '以太坊', 8, 1, 2),
       ('USDT', '泰达币', 6, 1, 3)
ON DUPLICATE KEY UPDATE `name`       = VALUES(`name`),
                        `scale`      = VALUES(`scale`),
                        `status`     = VALUES(`status`),
                        `sort_order` = VALUES(`sort_order`);

INSERT INTO `pair_symbol` (`symbol`, `base_currency`, `quote_currency`,
                           `price_scale`, `quantity_scale`,
                           `min_order_quantity`, `min_order_amount`,
                           `maker_fee_bps`, `taker_fee_bps`,
                           `status`, `sort_order`)
VALUES ('BTC-USDT', 'BTC', 'USDT', 2, 8, 0, 0, 10, 20, 1, 1),
       ('ETH-USDT', 'ETH', 'USDT', 2, 8, 0, 0, 10, 20, 1, 2)
ON DUPLICATE KEY UPDATE `base_currency`      = VALUES(`base_currency`),
                        `quote_currency`     = VALUES(`quote_currency`),
                        `price_scale`        = VALUES(`price_scale`),
                        `quantity_scale`     = VALUES(`quantity_scale`),
                        `min_order_quantity` = VALUES(`min_order_quantity`),
                        `min_order_amount`   = VALUES(`min_order_amount`),
                        `maker_fee_bps`      = VALUES(`maker_fee_bps`),
                        `taker_fee_bps`      = VALUES(`taker_fee_bps`),
                        `status`             = VALUES(`status`),
                        `sort_order`         = VALUES(`sort_order`);
