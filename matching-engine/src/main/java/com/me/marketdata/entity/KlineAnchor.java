package com.me.marketdata.entity;

import java.time.Instant;

/**
 * K 线锚点：每个 {@code (symbol, interval)} 一条记录，指向已经计算到的最新 bucket。
 *
 * <p>锚点之前的 bucket 都已经有 K 线（有成交的完整快照，或没成交时补出的平线），
 * 锚点之后还没有计算。进程重启或内存状态缺失时，按锚点恢复聚合位置：
 * 新成交 bucket 与锚点相等说明当前 K 线已存在，直接取出更新；晚于锚点则把中间
 * 没有成交的 bucket 补成平线，再把锚点推进到新 bucket。
 */
public record KlineAnchor(String symbol, KlineInterval interval, Instant bucketStart) {
}
