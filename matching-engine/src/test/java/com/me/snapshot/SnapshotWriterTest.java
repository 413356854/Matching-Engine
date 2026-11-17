package com.me.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.me.order.dto.OrderStateSnapshot;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

class SnapshotWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesChecksummedSnapshotAtomically() {
        SnapshotProperties properties = new SnapshotProperties(
                tempDir.toString(), 10_000, 1_000, 1_000, true, false);
        SnapshotWriter writer = new SnapshotWriter(properties);
        SnapshotPayload payload = new SnapshotPayload(
                "test-instance",
                "2026-01-01T00:00:00Z",
                11L,
                22L,
                new OrderStateSnapshot(List.of(), 42L),
                List.of(),
                List.of(),
                List.of(new TopicPartitionOffset(
                        "me.order.commands-batch", "order-command-consumer", 0, 7L)),
                List.of(
                        new ReplayCheckpoint("me.account.offset-seq-batch", 0, 5L),
                        new ReplayCheckpoint("me.order.offset-seq-batch", 0, 9L)));

        SnapshotWriter.SnapshotWriteResult result =
                writer.write(payload, Instant.parse(payload.createdAt()));

        assertTrue(Files.exists(result.path()));
        assertTrue(result.sizeBytes() > 0);
        assertFalse(result.checksum().isBlank());
        assertFalse(Files.exists(result.path().getParent().resolve(
                result.path().getFileName() + ".tmp")));

        SnapshotFile file = JsonMapper.builder().build()
                .readValue(result.path(), SnapshotFile.class);
        assertEquals(SnapshotFile.CURRENT_FORMAT_VERSION, file.formatVersion());
        assertEquals(result.checksum(), file.checksum());
        assertEquals(payload, file.payload());
    }
}
