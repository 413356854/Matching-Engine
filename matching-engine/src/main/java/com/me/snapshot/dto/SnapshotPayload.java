package com.me.snapshot.dto;

import com.me.account.vo.AccountView;
import com.me.matching.vo.OrderBookSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import java.util.List;

/**
 * 一份备份快照的完整内容。
 *
 * <p>账户、委托、订单簿三个内存结构与 {@code accountSeq} / {@code orderSeq} 水位、
 * 六条状态机输入 topic 的 offset 必须来自同一个静止点；{@code inputOffsets} 决定
 * 恢复时从哪里开始重放。
 *
 * <p>{@code replayCheckpoints} 记录两个 offset-seq topic 在切点的端位点：恢复时从这里
 * 开始读取映射，再按 {@code seq} 过滤已经包含在快照里的记录。<b>格式版本 2 起新增</b>。
 */
public record SnapshotPayload(
        String instanceId,
        String createdAt,
        long accountSeq,
        long orderSeq,
        OrderStateSnapshot orderState,
        List<AccountView> accounts,
        List<OrderBookSnapshot> books,
        List<TopicPartitionOffset> inputOffsets,
        List<ReplayCheckpoint> replayCheckpoints) {
}
