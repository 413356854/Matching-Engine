package com.me.admin.service;

import com.me.shared.event.MatchPairEvent;
import java.util.List;

/**
 * 用户成交记录归档：把撮合对拆成 taker / maker 两条记录写入 QuestDB {@code user_trade}。
 */
public interface UserTradeArchiveService {

    /** 批量归档一个 Kafka poll 批次中的撮合对；DEDUP 键保证重投不重复。 */
    void archiveBatch(List<MatchPairEvent> events);
}
