package com.me.admin.config;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import jakarta.annotation.PreDestroy;
import java.util.function.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * QuestDB 官方 Java 客户端写入通道（QWP over WebSocket，默认）。
 *
 * <p>与 JDBC/PostgreSQL wire 相比，QWP 是列式二进制批量写入协议；{@code QuestDB} facade
 * 负责连接池、重连与缓冲。每个写入批次借一个 {@link Sender}，写完后
 * {@link Sender#flushAndGetSequence()} + {@link Sender#awaitAckedFsn(long, long)} 等服务端
 * ack 再返回，调用方（Kafka 消费者）看到方法成功后才提交 offset，保证不丢数据。
 *
 * <p>{@code app.questdb.write-mode=jdbc} 时本通道关闭，仓库退回 JDBC batch；用于 A/B 对比
 * 或 QWP 不可用时回滚。
 */
@Component
public class QuestDbIngestClient {

    /** 单批等待 QuestDB ack 的上限；超时抛异常，Kafka 事务回滚并重投。 */
    static final long ACK_TIMEOUT_MS = 30_000;

    private final QuestDB db;
    private final boolean enabled;

    @Autowired
    public QuestDbIngestClient(QuestDbProperties properties) {
        this(connect(properties), properties.clientWriteEnabled());
    }

    /** 测试用：注入已构造的 facade。 */
    QuestDbIngestClient(QuestDB db, boolean enabled) {
        this.db = db;
        this.enabled = enabled;
    }

    private static QuestDB connect(QuestDbProperties properties) {
        if (!properties.clientWriteEnabled()) {
            return null;
        }
        return QuestDB.connect(properties.ingestConfString());
    }

    public boolean enabled() {
        return enabled;
    }

    /**
     * 在一个借出的 {@link Sender} 上写一批行，并等待服务端 ack。
     *
     * @param writer 逐行调用 sender.table(...).columns(...).at(...)
     */
    public void writeBatch(Consumer<Sender> writer) {
        if (!enabled || db == null) {
            throw new IllegalStateException("QuestDB QWP ingest client is disabled");
        }
        try (Sender sender = db.borrowSender()) {
            writer.accept(sender);
            long fsn = sender.flushAndGetSequence();
            if (!sender.awaitAckedFsn(fsn, ACK_TIMEOUT_MS)) {
                throw new IllegalStateException(
                        "QuestDB QWP batch was not acknowledged within " + ACK_TIMEOUT_MS + " ms");
            }
        }
    }

    @PreDestroy
    public void close() {
        if (db != null) {
            db.close();
        }
    }
}
