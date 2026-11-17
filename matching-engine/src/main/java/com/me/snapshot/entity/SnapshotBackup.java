package com.me.snapshot.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 备份快照记录表 {@code snapshot_backup}。
 *
 * <p>每次成功写完快照文件后插入一条，记录文件路径、校验和、seq 水位与统计信息；
 * 恢复 offset 存放在 {@link SnapshotBackupOffset}。
 */
@TableName("snapshot_backup")
public class SnapshotBackup {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 快照文件格式版本。 */
    private Integer formatVersion;

    /** 生成快照的实例 ID。 */
    private String instanceId;

    /** 快照文件绝对路径。 */
    private String filePath;

    /** 快照文件大小（字节）。 */
    private Long fileSizeBytes;

    /** payload 的 SHA-256。 */
    private String checksum;

    /** 快照切点时间（UTC）。 */
    private LocalDateTime snapshotCreatedAt;

    /** 账户模块 seq 水位。 */
    private Long accountSeq;

    /** 委托模块 seq 水位。 */
    private Long orderSeq;

    /** 账户记录数。 */
    private Integer accountCount;

    /** 委托记录数。 */
    private Integer orderCount;

    /** 订单簿数量。 */
    private Integer bookCount;

    /** 恢复 offset 条数。 */
    private Integer offsetCount;

    /** 快照总耗时（毫秒）。 */
    private Long durationMs;

    /** 记录时间（UTC）。 */
    private LocalDateTime createdAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(Integer formatVersion) {
        this.formatVersion = formatVersion;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(Long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public LocalDateTime getSnapshotCreatedAt() {
        return snapshotCreatedAt;
    }

    public void setSnapshotCreatedAt(LocalDateTime snapshotCreatedAt) {
        this.snapshotCreatedAt = snapshotCreatedAt;
    }

    public Long getAccountSeq() {
        return accountSeq;
    }

    public void setAccountSeq(Long accountSeq) {
        this.accountSeq = accountSeq;
    }

    public Long getOrderSeq() {
        return orderSeq;
    }

    public void setOrderSeq(Long orderSeq) {
        this.orderSeq = orderSeq;
    }

    public Integer getAccountCount() {
        return accountCount;
    }

    public void setAccountCount(Integer accountCount) {
        this.accountCount = accountCount;
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
    }

    public Integer getBookCount() {
        return bookCount;
    }

    public void setBookCount(Integer bookCount) {
        this.bookCount = bookCount;
    }

    public Integer getOffsetCount() {
        return offsetCount;
    }

    public void setOffsetCount(Integer offsetCount) {
        this.offsetCount = offsetCount;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
