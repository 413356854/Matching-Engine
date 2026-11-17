-- =====================================================================
-- 备份快照记录表 snapshot_backup / snapshot_backup_offset
-- 库：db_exchange（MySQL 8.0.17，utf8mb4 / utf8mb4_0900_ai_ci）
-- 说明：
--   1. snapshot_backup 保存每次成功快照的文件路径、校验和、seq 水位与统计信息；
--   2. snapshot_backup_offset 保存恢复所需的六条状态机输入 topic 每个分区的
--      next offset（快照切点时的消费组 committed offset）；
--   3. 脚本幂等，可重复执行。
-- =====================================================================

CREATE TABLE IF NOT EXISTS `snapshot_backup` (
    `id`                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    `format_version`      INT             NOT NULL COMMENT '快照文件格式版本',
    `instance_id`         VARCHAR(64)     NOT NULL COMMENT '生成快照的实例 ID',
    `file_path`           VARCHAR(1024)   NOT NULL COMMENT '快照文件绝对路径',
    `file_size_bytes`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '快照文件大小（字节）',
    `checksum`            CHAR(64)        NOT NULL COMMENT 'payload 的 SHA-256',
    `snapshot_created_at` DATETIME(3)     NOT NULL COMMENT '快照切点时间（UTC）',
    `account_seq`         BIGINT UNSIGNED NOT NULL COMMENT '账户模块 seq 水位',
    `order_seq`           BIGINT UNSIGNED NOT NULL COMMENT '委托模块 seq 水位',
    `account_count`       INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '账户记录数',
    `order_count`         INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '委托记录数',
    `book_count`          INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '订单簿数量',
    `offset_count`        INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '恢复 offset 条数',
    `duration_ms`         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '快照总耗时（毫秒）',
    `created_at`          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录时间（UTC）',
    PRIMARY KEY (`id`),
    KEY `idx_snapshot_backup_snapshot_created` (`snapshot_created_at`),
    KEY `idx_snapshot_backup_created` (`created_at`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '备份快照记录';

CREATE TABLE IF NOT EXISTS `snapshot_backup_offset` (
    `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    `backup_id`      BIGINT UNSIGNED NOT NULL COMMENT 'snapshot_backup.id',
    `topic`          VARCHAR(128)    NOT NULL COMMENT '状态机输入 topic',
    `consumer_group` VARCHAR(128)    NOT NULL COMMENT '消费组',
    `partition_no`   INT             NOT NULL COMMENT '分区号',
    `next_offset`    BIGINT          NOT NULL COMMENT '恢复起点（committed offset）',
    `created_at`     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录时间（UTC）',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_snapshot_backup_offset`
        (`backup_id`, `topic`, `consumer_group`, `partition_no`),
    KEY `idx_snapshot_backup_offset_backup` (`backup_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '备份快照恢复 offset';
