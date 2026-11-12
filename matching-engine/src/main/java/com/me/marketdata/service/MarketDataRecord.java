package com.me.marketdata.service;

import com.me.shared.event.MarketDataEvent;

/**
 * 一条待归档行情及其 Kafka 位置。
 *
 * <p>{@code partition + offset} 是承载该行情的<b>批量消息</b>的位置；同一批量消息内的多笔
 * 成交共享同一组位置。service 以整条 Kafka 记录为水位单位，同一条记录重投时跳过已聚合的
 * 周期，但记录内后续成交不会被误判为重复。
 */
public record MarketDataRecord(MarketDataEvent event, int partition, long offset) {
}
