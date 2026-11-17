package com.me.snapshot;

import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import tools.jackson.databind.json.JsonMapper;

/**
 * 快照文件读写与校验和的唯一入口。
 *
 * <p>写入端和读取端必须使用同一个 {@link JsonMapper} 配置：校验和覆盖的是
 * <b>payload 的紧凑 JSON 字节</b>，读取端要先把 payload 反序列化再重新序列化后比对，
 * 而不是对文件中的 payload 片段做哈希。收拢在这里避免两边格式漂移。
 */
public final class SnapshotCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private SnapshotCodec() {
    }

    public static JsonMapper mapper() {
        return MAPPER;
    }

    public static byte[] payloadBytes(SnapshotPayload payload) {
        return MAPPER.writeValueAsBytes(payload);
    }

    /** 读取快照文件外层结构；解析失败抛出 {@link SnapshotRecoveryException}。 */
    public static SnapshotFile read(Path path) {
        try {
            SnapshotFile file = MAPPER.readValue(path, SnapshotFile.class);
            if (file == null) {
                throw new SnapshotRecoveryException("snapshot file is empty: " + path);
            }
            return file;
        } catch (SnapshotRecoveryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SnapshotRecoveryException(
                    "failed to parse snapshot file " + path + ": " + e.getMessage(), e);
        }
    }

    /**
     * 校验 payload 的 SHA-256 与文件声明一致。
     *
     * @throws SnapshotRecoveryException 校验和不匹配
     */
    public static void verifyChecksum(SnapshotFile file, Path path) {
        if (file.payload() == null) {
            throw new SnapshotRecoveryException("snapshot file has no payload: " + path);
        }
        String expected = file.checksum();
        if (expected == null || expected.isBlank()) {
            throw new SnapshotRecoveryException("snapshot file has no checksum: " + path);
        }
        String actual = sha256(payloadBytes(file.payload()));
        if (!expected.equalsIgnoreCase(actual)) {
            throw new SnapshotRecoveryException("snapshot checksum mismatch: path=" + path
                    + ", expected=" + expected + ", actual=" + actual);
        }
    }

    public static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
