package com.me.marketdata.mq;

import com.me.marketdata.service.MarketDataRecord;
import com.me.marketdata.service.MarketDataService;
import com.me.shared.event.MatchPairBatch;
import com.me.shared.event.MatchPairEvent;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * 消费委托模块进程内撮合后输出的撮合对批量消息，生成 K 线并保存成交明细到 QuestDB。
 *
 * <p>一条 {@link MatchPairBatch} 消息包含一个 symbol 的一批成交（生产端按 symbol 分组，
 * key=symbol），批内顺序即成交顺序。同一批次消息内的成交共享 {@code (partition, offset)}：
 * service 以整条 Kafka 记录为水位单位，首次遇到时判断该记录是否已聚合，避免同一批次内的
 * 后续成交被误判为重复。
 *
 * <p>批量消费一个 poll 批次一个 Kafka 事务，整批一次 {@code ingestBatch} 批量写库；
 * 处理失败异常向上抛出，整批回滚重投；`md_trade` DEDUP 与 K 线 offset 水位保证重投不重复累计。
 */
@Component
public class MarketDataListener {

    private final MarketDataService marketDataService;

    public MarketDataListener(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    public void onMatchPair(List<ConsumerRecord<String, MatchPairBatch>> records) {
        List<MarketDataRecord> batch = new ArrayList<>();
        for (ConsumerRecord<String, MatchPairBatch> record : records) {
            MatchPairBatch message = record.value();
            if (message == null) {
                continue;
            }
            for (MatchPairEvent event : message.getMsgList()) {
                batch.add(new MarketDataRecord(event, record.partition(), record.offset()));
            }
        }
        marketDataService.ingestBatch(batch);
    }
}
