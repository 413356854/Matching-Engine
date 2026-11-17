import { useEffect, useState, type MouseEvent } from 'react';
import {
  createSnapshot,
  fetchSnapshotDetail,
  fetchSnapshots,
  isAbortError,
  type SnapshotBackup,
  type SnapshotBackupDetail,
} from '../api';
import { describeError } from '../errors';
import { formatDateTime } from '../format';
import { RefreshIcon } from './icons';
import type { ToastKind } from './ToastStack';

interface SnapshotAdminProps {
  notify: (kind: ToastKind, text: string) => void;
}

const LIST_LIMIT = 50;

function clockText(value: Date): string {
  return value.toLocaleTimeString('zh-CN', { hour12: false });
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '--';
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) {
    return '--';
  }
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms} ms`;
}

export function SnapshotAdmin({ notify }: SnapshotAdminProps) {
  const [backups, setBackups] = useState<SnapshotBackup[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<Date | null>(null);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [creating, setCreating] = useState(false);

  const [detailId, setDetailId] = useState<number | null>(null);
  const [detail, setDetail] = useState<SnapshotBackupDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const list = await fetchSnapshots(LIST_LIMIT, controller.signal);
        if (!controller.signal.aborted) {
          setBackups(list);
          setLoadedAt(new Date());
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setBackups([]);
          setError(describeError(loadError));
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => controller.abort();
  }, [refreshNonce]);

  useEffect(() => {
    if (detailId === null) {
      return;
    }
    const controller = new AbortController();
    setDetailLoading(true);
    setDetailError(null);
    setDetail(null);

    const load = async () => {
      try {
        const value = await fetchSnapshotDetail(detailId, controller.signal);
        if (!controller.signal.aborted) {
          setDetail(value);
        }
      } catch (loadError) {
        if (!isAbortError(loadError) && !controller.signal.aborted) {
          setDetailError(describeError(loadError));
        }
      } finally {
        if (!controller.signal.aborted) {
          setDetailLoading(false);
        }
      }
    };

    void load();
    return () => controller.abort();
  }, [detailId]);

  useEffect(() => {
    if (detailId === null) {
      return;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setDetailId(null);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [detailId]);

  const handleCreate = async () => {
    if (creating) {
      return;
    }
    setCreating(true);
    try {
      const result = await createSnapshot();
      if (!result.success) {
        notify('error', result.error ?? '快照失败');
        return;
      }
      notify(
        'success',
        `快照 #${result.backupId ?? '?'} 完成：${result.path ?? '文件已写入'}`,
      );
      setRefreshNonce((value) => value + 1);
    } catch (createError) {
      notify('error', describeError(createError));
    } finally {
      setCreating(false);
    }
  };

  const handleBackdrop = (event: MouseEvent<HTMLDivElement>) => {
    if (event.target === event.currentTarget) {
      setDetailId(null);
    }
  };

  return (
    <section className="panel admin-panel">
      <div className="panel-header admin-panel-header">
        <div>
          <h2>备份快照</h2>
          <span className="panel-sub">
            MySQL snapshot_backup · 文件路径 / seq 水位 / 逐分区恢复 offset
          </span>
        </div>
        <div className="admin-header-actions">
          <button
            type="button"
            className="icon-button"
            title="刷新快照列表"
            aria-label="刷新快照列表"
            disabled={loading || creating}
            onClick={() => setRefreshNonce((value) => value + 1)}
          >
            <RefreshIcon spinning={loading} />
          </button>
          <button
            type="button"
            className="admin-button primary"
            disabled={creating}
            onClick={handleCreate}
          >
            {creating ? '备份中…' : '立即备份'}
          </button>
        </div>
      </div>

      {error && <div className="admin-error">{error}</div>}

      <div className="admin-summary">
        <span>
          共 <b>{backups.length}</b> 条
        </span>
        <span>
          {loading
            ? '查询中…'
            : loadedAt
              ? `最近刷新 ${clockText(loadedAt)}`
              : '尚未查询'}
        </span>
        <span className="admin-summary-note">
          {creating
            ? '备份执行中，下单 / 撤单 / 充值入口暂停…'
            : '备份会短暂暂停入口消费；记录写入 MySQL snapshot_backup'}
        </span>
      </div>

      <div className="admin-table-wrap">
        <table className="admin-table snapshot-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>快照时间</th>
              <th className="align-right">账户</th>
              <th className="align-right">委托</th>
              <th className="align-right">订单簿</th>
              <th className="align-right">offset</th>
              <th>seq 水位</th>
              <th className="align-right">文件大小</th>
              <th className="align-right">耗时</th>
              <th>文件路径</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {backups.map((backup) => (
              <tr
                key={backup.id}
                title="查看快照详情"
                onClick={() => setDetailId(backup.id)}
              >
                <td className="mono">#{backup.id}</td>
                <td className="mono muted">{formatDateTime(backup.snapshotCreatedAt)}</td>
                <td className="mono align-right">{backup.accountCount}</td>
                <td className="mono align-right">{backup.orderCount}</td>
                <td className="mono align-right">{backup.bookCount}</td>
                <td className="mono align-right">{backup.offsetCount}</td>
                <td className="mono">
                  A {backup.accountSeq} / O {backup.orderSeq}
                </td>
                <td className="mono align-right">{formatBytes(backup.fileSizeBytes)}</td>
                <td className="mono align-right">{formatDuration(backup.durationMs)}</td>
                <td>
                  <span className="mono snapshot-path" title={backup.filePath}>
                    {backup.filePath}
                  </span>
                </td>
                <td>
                  <button
                    type="button"
                    className="admin-button"
                    onClick={(event) => {
                      event.stopPropagation();
                      setDetailId(backup.id);
                    }}
                  >
                    详情
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {!loading && backups.length === 0 && (
          <div className="admin-empty">
            {error
              ? '查询失败，请确认 MySQL 表 snapshot_backup 已创建、后端已启动后重试。'
              : '暂无快照记录。点击「立即备份」生成第一份快照。'}
          </div>
        )}
      </div>

      {detailId !== null && (
        <div className="modal-backdrop" onMouseDown={handleBackdrop}>
          <div
            className="modal-card snapshot-detail-card"
            role="dialog"
            aria-modal="true"
            aria-labelledby="snapshot-detail-title"
          >
            <div className="modal-head">
              <div>
                <h2 id="snapshot-detail-title">快照 #{detailId}</h2>
                <p className="modal-sub">
                  恢复水位与六条状态机输入 topic 的逐分区 offset
                </p>
              </div>
              <button
                type="button"
                className="modal-close"
                onClick={() => setDetailId(null)}
                aria-label="关闭"
              >
                ×
              </button>
            </div>

            <div className="modal-body snapshot-detail-body">
              {detailLoading && <div className="admin-empty">读取中…</div>}
              {detailError && <div className="modal-error">{detailError}</div>}
              {detail && (
                <>
                  <div className="snapshot-meta">
                    <div>
                      <span>快照时间</span>
                      <b>{formatDateTime(detail.backup.snapshotCreatedAt)}</b>
                    </div>
                    <div>
                      <span>记录时间</span>
                      <b>{formatDateTime(detail.backup.createdAt)}</b>
                    </div>
                    <div>
                      <span>实例 / 格式版本</span>
                      <b>
                        {detail.backup.instanceId} / v{detail.backup.formatVersion}
                      </b>
                    </div>
                    <div>
                      <span>账户 seq / 委托 seq</span>
                      <b>
                        {detail.backup.accountSeq} / {detail.backup.orderSeq}
                      </b>
                    </div>
                    <div>
                      <span>账户 / 委托 / 订单簿</span>
                      <b>
                        {detail.backup.accountCount} / {detail.backup.orderCount} /{' '}
                        {detail.backup.bookCount}
                      </b>
                    </div>
                    <div>
                      <span>文件大小 / 耗时</span>
                      <b>
                        {formatBytes(detail.backup.fileSizeBytes)} /{' '}
                        {formatDuration(detail.backup.durationMs)}
                      </b>
                    </div>
                    <div className="span-3">
                      <span>文件路径</span>
                      <b>{detail.backup.filePath}</b>
                    </div>
                    <div className="span-3">
                      <span>SHA-256</span>
                      <b>{detail.backup.checksum}</b>
                    </div>
                  </div>

                  <div className="snapshot-detail-scroll">
                    <table className="admin-table snapshot-offset-table">
                      <thead>
                        <tr>
                          <th>topic</th>
                          <th>消费组</th>
                          <th className="align-right">分区</th>
                          <th className="align-right">next offset</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.offsets.map((offset) => (
                          <tr key={`${offset.topic}-${offset.group}-${offset.partition}`}>
                            <td className="mono">{offset.topic}</td>
                            <td className="mono muted">{offset.group}</td>
                            <td className="mono align-right">{offset.partition}</td>
                            <td className="mono align-right">{offset.nextOffset}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {detail.offsets.length === 0 && (
                      <div className="admin-empty">该快照没有记录恢复 offset。</div>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
