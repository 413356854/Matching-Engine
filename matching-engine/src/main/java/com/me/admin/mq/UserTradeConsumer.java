package com.me.admin.mq;

import com.me.admin.service.UserTradeArchiveService;
import com.me.shared.event.MatchPairBatch;
import com.me.shared.event.MatchPairEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.match-pair-batch}，把每个撮合对拆成 taker / maker 两条用户成交记录
 * 批量归档到 QuestDB {@code user_trade}。
 *
 * <p><b>独立消费组</b>：与行情模块的 {@code marketdata-trade-consumer} 不同组，
 * 两边都消费全量撮合对，互不影响 offset。
 *
 * <p><b>批量消费与批量写入</b>：{@code batch="true"}，一个 poll 批次一个 Kafka 事务；
 * poll 到的多条 record 先全部展开，再按 {@link #INSERT_BATCH_SIZE} 个撮合对分块写入 QuestDB，
 * 摊薄 QWP flush + ack 的固定开销。
 *
 * <p><b>幂等</b>：QuestDB {@code user_trade} 按 {@code (executed_at, trade_id, order_id)}
 * DEDUP，同一撮合对重投不会产生重复行；消费端再用有界 {@code symbol + tradeId} 集合跳过
 * 已成功归档的成交，减少重复写入。集合只在整块写入成功后更新。
 */
@Component
public class UserTradeConsumer {

    /** 已成功归档的成交窗口；只需覆盖批量重试窗口。 */
    static final int SEEN_TRADE_LIMIT = 65_536;

    /** 单次 QuestDB/QWP 写入的最大撮合对数（每对写 taker / maker 两行）。 */
    static final int INSERT_BATCH_SIZE = 20_000;

    private final UserTradeArchiveService archiveService;

    /**
     * 已成功写入 QuestDB 的成交身份（symbol + tradeId）。
     *
     * <p>user-trade 可以有多个 consumer 并发消费（key=symbol 分区），本集合被多个线程共享，
     * 因此必须同步访问；同一 symbol 只会落在一个分区，正常情况下不会跨线程竞争。
     */
    private final Set<String> seenTrades = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > SEEN_TRADE_LIMIT;
                }
            }));

    public UserTradeConsumer(UserTradeArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    public void onMatchPair(List<ConsumerRecord<String, MatchPairBatch>> records) {
        List<MatchPairEvent> pending = new ArrayList<>(1024);
        Set<String> batchKeys = new HashSet<>();
        for (ConsumerRecord<String, MatchPairBatch> record : records) {
            MatchPairBatch message = record.value();
            if (message == null) {
                continue;
            }
            for (MatchPairEvent event : message.getMsgList()) {
                String key = tradeKey(event);
                if (seenTrades.contains(key) || !batchKeys.add(key)) {
                    continue;
                }
                pending.add(event);
                if (pending.size() >= INSERT_BATCH_SIZE) {
                    archive(pending);
                    pending = new ArrayList<>(1024);
                }
            }
        }
        if (!pending.isEmpty()) {
            archive(pending);
        }
    }

    /**
     * 写入一个分块。成功后才把该块的成交身份记入去重窗口；失败向上抛，Kafka 事务回滚，
     * 整批重投时已写入前缀会被跳过（QuestDB DEDUP 也会兜底）。
     */
    private void archive(List<MatchPairEvent> events) {
        archiveService.archiveBatch(events);
        events.forEach(event -> seenTrades.add(tradeKey(event)));
    }

    private static String tradeKey(MatchPairEvent event) {
        return event.symbol() + ":" + event.tradeId();
    }
}
