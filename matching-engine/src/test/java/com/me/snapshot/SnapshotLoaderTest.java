package com.me.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SnapshotLoaderTest {

    @TempDir
    Path tempDir;

    private final SnapshotLoader loader = new SnapshotLoader();

    @Test
    void loadsRoundTrippedSnapshot() {
        SnapshotPayload payload = SnapshotTestData.validPayload();
        Path path = write(payload);

        SnapshotLoader.LoadedSnapshot loaded = loader.load(path);

        assertEquals(payload, loaded.payload());
        assertEquals(payload.replayCheckpoints(), loaded.payload().replayCheckpoints());
        assertTrue(loaded.file().checksum().length() == 64);
    }

    @Test
    void rejectsChecksumMismatch() throws Exception {
        SnapshotPayload payload = SnapshotTestData.validPayload();
        SnapshotFile tampered = new SnapshotFile(
                SnapshotFile.CURRENT_FORMAT_VERSION, "0".repeat(64), payload);
        Path path = tempDir.resolve("tampered.json");
        Files.writeString(path, SnapshotCodec.mapper().writeValueAsString(tampered));

        SnapshotRecoveryException error = assertThrows(SnapshotRecoveryException.class,
                () -> loader.load(path));
        assertTrue(error.getMessage().contains("checksum"));
    }

    @Test
    void rejectsOlderFormatVersion() throws Exception {
        SnapshotPayload payload = SnapshotTestData.validPayload();
        SnapshotFile old = new SnapshotFile(1,
                SnapshotCodec.sha256(SnapshotCodec.payloadBytes(payload)), payload);
        Path path = tempDir.resolve("old.json");
        Files.writeString(path, SnapshotCodec.mapper().writeValueAsString(old));

        SnapshotRecoveryException error = assertThrows(SnapshotRecoveryException.class,
                () -> loader.load(path));
        assertTrue(error.getMessage().contains("format version"));
    }

    @Test
    void rejectsMissingReplayCheckpoints() {
        Path path = write(SnapshotTestData.withoutCheckpoints());

        SnapshotRecoveryException error = assertThrows(SnapshotRecoveryException.class,
                () -> loader.load(path));
        assertTrue(error.getMessage().contains("replayCheckpoints"));
    }

    @Test
    void rejectsInvariantViolation() {
        Path path = write(SnapshotTestData.withInvariantViolation());

        SnapshotRecoveryException error = assertThrows(SnapshotRecoveryException.class,
                () -> loader.load(path));
        assertTrue(error.getMessage().contains("invariant")
                || error.getMessage().contains("accountFrozen"));
    }

    @Test
    void rejectsOpenOrderWithoutRestingBookOrder() {
        Path path = write(SnapshotTestData.withoutBooks());

        SnapshotRecoveryException error = assertThrows(SnapshotRecoveryException.class,
                () -> loader.load(path));
        assertTrue(error.getMessage().contains("not resting"));
    }

    private Path write(SnapshotPayload payload) {
        SnapshotProperties properties = new SnapshotProperties(
                tempDir.toString(), 10_000, 1_000, 1_000, true, false);
        SnapshotWriter writer = new SnapshotWriter(properties);
        return writer.write(payload, Instant.parse(payload.createdAt())).path();
    }
}
