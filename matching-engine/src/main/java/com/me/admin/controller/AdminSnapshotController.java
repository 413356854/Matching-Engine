package com.me.admin.controller;

import com.me.admin.vo.AdminSnapshotDetailView;
import com.me.admin.vo.AdminSnapshotQueryView;
import com.me.admin.vo.AdminSnapshotRecoveryView;
import com.me.admin.vo.AdminSnapshotView;
import com.me.snapshot.SnapshotInProgressException;
import com.me.snapshot.SnapshotService;
import com.me.snapshot.SnapshotUnavailableException;
import com.me.snapshot.dto.SnapshotBackupDetail;
import com.me.snapshot.dto.SnapshotResult;
import com.me.snapshot.recovery.RecoveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端备份快照接口。
 *
 * <p>{@code POST /api/v1/admin/snapshots} 同步执行一次完整快照：持有三个入口锁、
 * 等待内部链路排空和 committer 提交、复制内存状态、校验一致性并原子写文件，
 * 最后把文件路径、seq 水位和逐分区恢复 offset 写入 MySQL
 * {@code snapshot_backup / snapshot_backup_offset}。
 * 快照完成前请求会阻塞，调用方应设置足够的 HTTP 超时。
 *
 * <p>{@code GET /api/v1/admin/snapshots?limit=50} 查询最近快照记录；
 * {@code GET /api/v1/admin/snapshots/{id}} 查询单次快照的详情和逐分区 offset。
 *
 * <p>同一时间只允许一个快照；并发调用返回 409。引擎 fatal、committer 停止、
 * 排空超时或一致性校验失败返回 503。
 */
@RestController
@RequestMapping("/api/v1/admin/snapshots")
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class AdminSnapshotController {

    private static final Logger log = LoggerFactory.getLogger(AdminSnapshotController.class);

    private final SnapshotService snapshotService;
    private final RecoveryState recoveryState;

    public AdminSnapshotController(SnapshotService snapshotService,
                                   RecoveryState recoveryState) {
        this.snapshotService = snapshotService;
        this.recoveryState = recoveryState;
    }

    /**
     * 启动恢复状态。恢复期间其他接口统一返回 503，本接口用于区分“恢复中”和“恢复失败”。
     */
    @GetMapping("/recovery")
    public ResponseEntity<AdminSnapshotRecoveryView> recovery() {
        return ResponseEntity.ok(AdminSnapshotRecoveryView.success(recoveryState.status()));
    }

    @GetMapping
    public ResponseEntity<AdminSnapshotQueryView> list(
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        if (limit <= 0) {
            return ResponseEntity.badRequest()
                    .body(AdminSnapshotQueryView.rejected("LIMIT_INVALID"));
        }
        try {
            return ResponseEntity.ok(AdminSnapshotQueryView.success(
                    snapshotService.listBackups(Math.min(limit, 200))));
        } catch (DataAccessException e) {
            log.error("admin snapshot list failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminSnapshotQueryView.rejected("QUERY_FAILED"));
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<AdminSnapshotDetailView> detail(
            @PathVariable("id") long id) {
        if (id <= 0) {
            return ResponseEntity.badRequest()
                    .body(AdminSnapshotDetailView.rejected("ID_INVALID"));
        }
        try {
            SnapshotBackupDetail detail = snapshotService.getBackup(id);
            if (detail == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(AdminSnapshotDetailView.rejected("NOT_FOUND"));
            }
            return ResponseEntity.ok(AdminSnapshotDetailView.success(
                    detail.backup(), detail.offsets()));
        } catch (DataAccessException e) {
            log.error("admin snapshot detail failed: id={}", id, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminSnapshotDetailView.rejected("QUERY_FAILED"));
        }
    }

    @PostMapping
    public ResponseEntity<AdminSnapshotView> createSnapshot() {
        try {
            SnapshotResult result = snapshotService.createSnapshot();
            return ResponseEntity.ok(AdminSnapshotView.success(result));
        } catch (SnapshotInProgressException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(AdminSnapshotView.failure(e.getMessage()));
        } catch (SnapshotUnavailableException e) {
            log.warn("snapshot unavailable: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminSnapshotView.failure(e.getMessage()));
        } catch (RuntimeException e) {
            log.error("snapshot failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(AdminSnapshotView.failure(e.getMessage()));
        }
    }
}
