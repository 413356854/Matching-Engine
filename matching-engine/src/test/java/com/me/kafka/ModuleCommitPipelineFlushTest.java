package com.me.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

class ModuleCommitPipelineFlushTest {

    @Test
    void flushWaitsUntilEnqueuedPackageIsCommitted() throws Exception {
        KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
        @SuppressWarnings("unchecked")
        KafkaProducer<String, Object> producer = mock(KafkaProducer.class);
        when(clientFactory.createProducer("test-committer", 0)).thenReturn(producer);

        ModuleCommitPipeline pipeline = new ModuleCommitPipeline(
                "test-committer", clientFactory, new FatalState(), 8, 2);
        pipeline.start();
        try {
            pipeline.enqueue(new SendPackage(
                    "test", List.of(), Map.of(), new ConsumerGroupMetadata("test-group")));
            pipeline.flush(2_000);

            assertEquals(0L, pipeline.inFlightPackages());
            verify(producer).beginTransaction();
            verify(producer).commitTransaction();
        } finally {
            pipeline.stop();
        }
    }
}
