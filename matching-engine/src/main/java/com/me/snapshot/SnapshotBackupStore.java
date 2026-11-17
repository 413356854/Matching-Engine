package com.me.snapshot;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.me.snapshot.dto.SnapshotBackupDetail;
import com.me.snapshot.dto.SnapshotBackupOffsetRecord;
import com.me.snapshot.dto.SnapshotBackupRecord;
import com.me.snapshot.entity.SnapshotBackup;
import com.me.snapshot.entity.SnapshotBackupOffset;
import com.me.snapshot.mapper.SnapshotBackupMapper;
import com.me.snapshot.mapper.SnapshotBackupOffsetMapper;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 快照元数据持久化：{@code snapshot_backup} + {@code snapshot_backup_offset}。
 *
 * <p>主表和 offset 表在同一个本地事务里写入；主表插入失败时不会留下半条记录。
 * 查询按 id 倒序返回最近快照，管理端详情页再按 backup_id 读取逐分区 offset。
 */
@Service
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class SnapshotBackupStore {

    private static final int MAX_LIST_LIMIT = 500;

    private final SnapshotBackupMapper backupMapper;
    private final SnapshotBackupOffsetMapper offsetMapper;

    public SnapshotBackupStore(SnapshotBackupMapper backupMapper,
                               SnapshotBackupOffsetMapper offsetMapper) {
        this.backupMapper = backupMapper;
        this.offsetMapper = offsetMapper;
    }

    /**
     * 在一个事务里写入快照主记录与全部 offset，返回自增主键。
     */
    @Transactional
    public long save(SnapshotBackup backup, List<SnapshotBackupOffset> offsets) {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        if (backup.getCreatedAt() == null) {
            backup.setCreatedAt(now);
        }
        backupMapper.insert(backup);
        Long backupId = backup.getId();
        if (backupId == null) {
            throw new IllegalStateException("snapshot_backup insert did not return an id");
        }
        for (SnapshotBackupOffset offset : offsets) {
            offset.setBackupId(backupId);
            if (offset.getCreatedAt() == null) {
                offset.setCreatedAt(now);
            }
            offsetMapper.insert(offset);
        }
        return backupId;
    }

    /** 最近快照列表，按 id 倒序；limit 会被夹取到 1..500。 */
    public List<SnapshotBackupRecord> list(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, MAX_LIST_LIMIT));
        LambdaQueryWrapper<SnapshotBackup> wrapper = new LambdaQueryWrapper<SnapshotBackup>()
                .orderByDesc(SnapshotBackup::getId)
                .last("LIMIT " + safeLimit);
        return backupMapper.selectList(wrapper).stream()
                .map(SnapshotBackupStore::toRecord)
                .toList();
    }

    /** 快照详情；不存在时返回 {@code null}。 */
    public SnapshotBackupDetail find(long id) {
        SnapshotBackup backup = backupMapper.selectById(id);
        if (backup == null) {
            return null;
        }
        List<SnapshotBackupOffset> offsets = offsetMapper.selectList(
                new LambdaQueryWrapper<SnapshotBackupOffset>()
                        .eq(SnapshotBackupOffset::getBackupId, id)
                        .orderByAsc(SnapshotBackupOffset::getTopic)
                        .orderByAsc(SnapshotBackupOffset::getConsumerGroup)
                        .orderByAsc(SnapshotBackupOffset::getPartitionNo));
        return new SnapshotBackupDetail(
                toRecord(backup),
                offsets.stream()
                        .map(offset -> new SnapshotBackupOffsetRecord(
                                offset.getTopic(), offset.getConsumerGroup(),
                                offset.getPartitionNo(), offset.getNextOffset()))
                        .toList());
    }

    private static SnapshotBackupRecord toRecord(SnapshotBackup backup) {
        return new SnapshotBackupRecord(
                backup.getId() == null ? 0L : backup.getId(),
                backup.getFormatVersion() == null ? 0 : backup.getFormatVersion(),
                backup.getInstanceId(),
                backup.getFilePath(),
                backup.getFileSizeBytes() == null ? 0L : backup.getFileSizeBytes(),
                backup.getChecksum(),
                backup.getSnapshotCreatedAt() == null
                        ? null
                        : backup.getSnapshotCreatedAt().toInstant(ZoneOffset.UTC).toString(),
                backup.getAccountSeq() == null ? 0L : backup.getAccountSeq(),
                backup.getOrderSeq() == null ? 0L : backup.getOrderSeq(),
                backup.getAccountCount() == null ? 0 : backup.getAccountCount(),
                backup.getOrderCount() == null ? 0 : backup.getOrderCount(),
                backup.getBookCount() == null ? 0 : backup.getBookCount(),
                backup.getOffsetCount() == null ? 0 : backup.getOffsetCount(),
                backup.getDurationMs() == null ? 0L : backup.getDurationMs(),
                backup.getCreatedAt() == null
                        ? null
                        : backup.getCreatedAt().toInstant(ZoneOffset.UTC).toString());
    }
}
