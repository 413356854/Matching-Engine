package com.me.snapshot.recovery;

import java.util.List;

/**
 * 两个模块各自的重放批次，均已按 seq 升序排列并完成冲突校验。
 */
public record ReplayMappings(List<ReplayBatch> orderBatches,
                             List<ReplayBatch> accountBatches) {

    public ReplayMappings {
        orderBatches = List.copyOf(orderBatches);
        accountBatches = List.copyOf(accountBatches);
    }

    public long orderRecordCount() {
        return orderBatches.stream().mapToLong(batch -> batch.mappings().size()).sum();
    }

    public long accountRecordCount() {
        return accountBatches.stream().mapToLong(batch -> batch.mappings().size()).sum();
    }

    public List<ReplayMapping> allMappings() {
        List<ReplayMapping> all = new java.util.ArrayList<>(
                (int) Math.min(Integer.MAX_VALUE, orderRecordCount() + accountRecordCount()));
        orderBatches.forEach(batch -> all.addAll(batch.mappings()));
        accountBatches.forEach(batch -> all.addAll(batch.mappings()));
        return all;
    }
}
