package com.me.snapshot;

import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 快照文件写入：SHA-256 校验和、临时文件 + fsync + 原子 rename。
 *
 * <p>写入期间不持有任何模块锁：调用方应在内存里准备好不可变的 {@link SnapshotPayload}
 * 后再写入。若进程在 rename 之前崩溃，旧快照保持不变，临时文件会被下次写入忽略。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class SnapshotWriter {

    private static final Logger log = LoggerFactory.getLogger(SnapshotWriter.class);
    private static final DateTimeFormatter FILE_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS").withZone(ZoneOffset.UTC);

    private final SnapshotProperties properties;

    public SnapshotWriter(SnapshotProperties properties) {
        this.properties = properties;
    }

    public SnapshotWriteResult write(SnapshotPayload payload, Instant createdAt) {
        byte[] payloadBytes = SnapshotCodec.payloadBytes(payload);
        String checksum = SnapshotCodec.sha256(payloadBytes);
        SnapshotFile file = new SnapshotFile(
                SnapshotFile.CURRENT_FORMAT_VERSION, checksum, payload);

        Path directory = Path.of(properties.directory()).toAbsolutePath().normalize();
        String name = "snapshot-" + FILE_TIME.format(createdAt)
                + "-a" + payload.accountSeq() + "-o" + payload.orderSeq()
                + "-" + UUID.randomUUID().toString().substring(0, 8) + ".json";
        Path target = directory.resolve(name);
        Path temporary = directory.resolve(name + ".tmp");
        try {
            Files.createDirectories(directory);
            // 先序列化成字节再写 channel：Jackson 的 writeValue(OutputStream) 默认会关闭
            // 目标流，导致后续 fsync 拿到的 FileChannel 已关闭。
            byte[] fileBytes = properties.prettyPrint()
                    ? SnapshotCodec.mapper().writerWithDefaultPrettyPrinter()
                            .writeValueAsBytes(file)
                    : SnapshotCodec.mapper().writeValueAsBytes(file);
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(fileBytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            moveAtomically(temporary, target);
            long size = Files.size(target);
            log.info("snapshot file written: path={}, size={}B, checksum={}, formatVersion={}",
                    target, size, checksum, SnapshotFile.CURRENT_FORMAT_VERSION);
            return new SnapshotWriteResult(target, checksum, size);
        } catch (IOException | RuntimeException e) {
            deleteQuietly(temporary);
            throw new IllegalStateException("failed to write snapshot " + target, e);
        }
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // 少数文件系统不支持原子 rename；退化为普通 move，调用方仍以临时文件 + 目标文件区分完整性。
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("failed to delete temporary snapshot file {}", path, e);
        }
    }

    /** 写入结果：最终路径、校验和与文件大小。 */
    public record SnapshotWriteResult(Path path, String checksum, long sizeBytes) {
    }
}
