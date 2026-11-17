package com.me.snapshot.recovery;

/**
 * offset-seq topic 中的一条映射：某条输入消费记录 (topic/partition/offset) 当时被分配到的
 * 全局序号。
 */
public record ReplayMapping(String topic, int partition, long offset, long seq) {
}
