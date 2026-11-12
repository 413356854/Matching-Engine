package com.me.currency.mq;

import com.me.currency.ingress.OrderBatchSender;
import com.me.kafka.KafkaClientFactory;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 下单批量消息的 Kafka 出口：把同一 symbol 的命令封装成一条
 * {@link OrderCommandBatch}，{@code key=symbol}，异步发送、不等待 broker ack。
 *
 * <p><b>顺序</b>：只由 {@code order-ingress-handler} 单线程调用，并固定使用同一个
 * 幂等、非事务 producer。同一 symbol 的批次按发送顺序进入同一分区，producer 的重试
 * 由幂等序列号保证不改变顺序。
 *
 * <p>发送失败分成两种：同步入队失败（producer 关闭、缓冲区满等）向上抛，由聚合线程
 * 记录日志；broker ack 失败在回调里记录。REST 下单已返回 202，不会同步感知发送结果，
 * 与逐条异步发送时的语义一致。
 */
@Component
public class OrderCommandBatchPublisher implements OrderBatchSender {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandBatchPublisher.class);

    private final KafkaClientFactory clientFactory;
    private final String topic;
    private final Object producerLock = new Object();

    private volatile KafkaProducer<String, Object> producer;
    private volatile boolean closed;

    public OrderCommandBatchPublisher(KafkaClientFactory clientFactory,
                                      @Value("${app.kafka.topics.order-commands}") String topic) {
        this.clientFactory = clientFactory;
        this.topic = topic;
    }

    @Override
    public void send(String symbol, List<OrderCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        if (closed) {
            throw new IllegalStateException("OrderCommandBatchPublisher is closed");
        }
        OrderCommandBatch batch = new OrderCommandBatch(commands);
        KafkaProducer<String, Object> current = producer();
        try {
            current.send(new ProducerRecord<>(topic, symbol, batch), (metadata, error) -> {
                if (error != null) {
                    log.error("order command batch publish failed: topic={}, symbol={}, size={}",
                            topic, symbol, commands.size(), error);
                }
            });
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                discard(current);
            }
            throw new IllegalStateException(
                    "order command batch enqueue failed: symbol=" + symbol
                            + ", size=" + commands.size(), e);
        }
    }

    /** 懒创建：应用启动不依赖 Kafka 可用，首个 flush 时才建 producer。 */
    private KafkaProducer<String, Object> producer() {
        KafkaProducer<String, Object> current = producer;
        if (current != null) {
            return current;
        }
        synchronized (producerLock) {
            if (closed) {
                throw new IllegalStateException("OrderCommandBatchPublisher is closed");
            }
            if (producer == null) {
                producer = clientFactory.createIdempotentProducer("order-batch", 0);
            }
            return producer;
        }
    }

    private void discard(KafkaProducer<String, Object> current) {
        synchronized (producerLock) {
            if (producer != current) {
                return;
            }
            producer = null;
            try {
                current.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("failed to close order batch producer", e);
            }
        }
    }

    private static boolean isFatal(Throwable throwable) {
        return throwable instanceof AuthorizationException
                || throwable instanceof OutOfOrderSequenceException
                || throwable instanceof InvalidProducerEpochException
                || throwable instanceof UnsupportedVersionException;
    }

    @PreDestroy
    public void close() {
        KafkaProducer<String, Object> current;
        synchronized (producerLock) {
            closed = true;
            current = producer;
            producer = null;
        }
        if (current != null) {
            try {
                current.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("failed to close order batch producer", e);
            }
        }
    }
}
