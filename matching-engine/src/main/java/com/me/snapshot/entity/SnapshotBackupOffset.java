package com.me.snapshot.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 备份快照恢复 offset 表 {@code snapshot_backup_offset}。
 *
 * <p>每个快照记录六条状态机输入 topic 的每个分区一行，{@code next_offset} 是
 * 快照切点时的消费组 committed offset。
 */
@TableName("snapshot_backup_offset")
public class SnapshotBackupOffset {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 所属快照 {@code snapshot_backup.id}。 */
    private Long backupId;

    /** 状态机输入 topic。 */
    private String topic;

    /** 消费组。 */
    private String consumerGroup;

    /** 分区号。 */
    private Integer partitionNo;

    /** 恢复起点（committed offset）。 */
    private Long nextOffset;

    /** 记录时间（UTC）。 */
    private LocalDateTime createdAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getBackupId() {
        return backupId;
    }

    public void setBackupId(Long backupId) {
        this.backupId = backupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Integer getPartitionNo() {
        return partitionNo;
    }

    public void setPartitionNo(Integer partitionNo) {
        this.partitionNo = partitionNo;
    }

    public Long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(Long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
