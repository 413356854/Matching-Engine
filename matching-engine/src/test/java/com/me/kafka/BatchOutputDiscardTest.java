package com.me.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class BatchOutputDiscardTest {

    @Test
    void discardModeDropsMessagesAndEnqueue() {
        BatchOutput output = BatchOutput.discard("snapshot-replay");

        output.sendBatchMessage("me.order.commands-batch", List.of(1, 2), values -> values);
        output.enqueue();

        assertEquals("snapshot-replay", output.source());
        assertTrue(output.records().isEmpty());
    }
}
