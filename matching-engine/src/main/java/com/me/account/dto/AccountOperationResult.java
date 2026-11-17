package com.me.account.dto;

import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.OrderFreezeResultEvent;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个账户操作批次的执行结果。
 *
 * <p>{@code offsets} 是本批每条 Kafka 输入记录与自己独立序号的对应关系，
 * {@code AccountOperationServiceImpl} 在 accountLock 内把它发往
 * {@code me.account.offset-seq-batch}；批次水位可由最后一条 offset 的
 * {@code accountSeq} 推导，不再单独保存。
 *
 * <p>{@code flows} 按输入记录/命令的原始顺序汇总，供 {@code me.funds-batch} 使用；
 * {@code freezeResults} 只在下单冻结链路非空，按 symbol 分组供
 * {@code me.order.freeze-results-batch} 使用。
 */
public record AccountOperationResult(
        List<AccountOffsetSeq> offsets,
        List<FundsFlowEvent> flows,
        Map<String, List<OrderFreezeResultEvent>> freezeResults) {

    public AccountOperationResult {
        offsets = offsets == null ? List.of() : List.copyOf(offsets);
        flows = flows == null ? List.of() : List.copyOf(flows);
        freezeResults = freezeResults == null || freezeResults.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(freezeResults));
    }

    /** 没有可执行命令或没有产生流水时的空结果。 */
    public static AccountOperationResult empty() {
        return new AccountOperationResult(List.of(), List.of(), Map.of());
    }
}
