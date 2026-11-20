package com.me.snapshot;

import com.me.account.vo.AccountView;
import com.me.matching.vo.BookOrderSnapshot;
import com.me.matching.vo.OrderBookSnapshot;
import com.me.matching.vo.PriceLevelSnapshot;
import com.me.order.dto.OrderSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotPayload;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.time.Instant;
import java.util.List;

/** 测试用最小合法快照：一个账户、一笔挂单、一个订单簿。 */
final class SnapshotTestData {

    static final Instant CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");

    private SnapshotTestData() {
    }

    static SnapshotPayload validPayload() {
        AccountView account = new AccountView(1L, "USDT", 900L, 100L, 3L);
        OrderSnapshot order = new OrderSnapshot(
                "req-1", 1L, 1L, "BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                10L, 10L, "USDT", 100L, 0L, 0L, OrderStatus.PAID, 2L,
                CREATED_AT.toString(), null, 0, 0, 0, false);
        BookOrderSnapshot bookOrder = new BookOrderSnapshot(
                1L, 1L, "BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                10L, 10L, 10L, CREATED_AT.toString());
        OrderBookSnapshot book = new OrderBookSnapshot(
                "BTC-USDT",
                List.of(new PriceLevelSnapshot(10L, List.of(bookOrder))),
                List.of(),
                List.of(1L),
                1L);
        return new SnapshotPayload(
                "test-instance",
                CREATED_AT.toString(),
                11L,
                22L,
                new OrderStateSnapshot(List.of(order), 42L),
                List.of(account),
                List.of(book),
                List.of(new TopicPartitionOffset("order-commands", "order-command-consumer", 0, 7L)),
                List.of(
                        new ReplayCheckpoint("account-offset-seq", 0, 5L),
                        new ReplayCheckpoint("order-offset-seq", 0, 9L)));
    }

    static SnapshotPayload withInvariantViolation() {
        SnapshotPayload valid = validPayload();
        AccountView wrong = new AccountView(1L, "USDT", 950L, 50L, 3L);
        return new SnapshotPayload(valid.instanceId(), valid.createdAt(), valid.accountSeq(),
                valid.orderSeq(), valid.orderState(), List.of(wrong), valid.books(),
                valid.inputOffsets(), valid.replayCheckpoints());
    }

    /** 部分成交收尾的委托：PARTIALLY_FILLED + finalAt 非空，不挂单，冻结已退回。 */
    static SnapshotPayload terminalPartialPayload() {
        AccountView account = new AccountView(1L, "USDT", 960L, 0L, 3L);
        OrderSnapshot order = new OrderSnapshot(
                "req-1", 1L, 1L, "BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                10L, 10L, "USDT", 100L, 4L, 40L, OrderStatus.PARTIALLY_FILLED, 4L,
                CREATED_AT.toString(), CREATED_AT.plusSeconds(1).toString(),
                0, 0, 0, false);
        OrderBookSnapshot book = new OrderBookSnapshot(
                "BTC-USDT", List.of(), List.of(), List.of(1L), 1L);
        return new SnapshotPayload(
                "test-instance",
                CREATED_AT.toString(),
                11L,
                22L,
                new OrderStateSnapshot(List.of(order), 42L),
                List.of(account),
                List.of(book),
                List.of(new TopicPartitionOffset(
                        "order-commands", "order-command-consumer", 0, 7L)),
                List.of(
                        new ReplayCheckpoint("account-offset-seq", 0, 5L),
                        new ReplayCheckpoint("order-offset-seq", 0, 9L)));
    }

    /** 未完成的市价单：IOC 收尾是同步的，静止快照中不应出现。 */
    static SnapshotPayload openMarketOrderPayload() {
        AccountView account = new AccountView(1L, "USDT", 900L, 100L, 3L);
        OrderSnapshot order = new OrderSnapshot(
                "req-1", 1L, 1L, "BTC-USDT", OrderSide.BUY, OrderType.MARKET,
                0L, 10L, "USDT", 100L, 0L, 0L, OrderStatus.PAID, 2L,
                CREATED_AT.toString(), null, 0, 0, 0, false);
        OrderBookSnapshot book = new OrderBookSnapshot(
                "BTC-USDT", List.of(), List.of(), List.of(1L), 1L);
        return new SnapshotPayload(
                "test-instance",
                CREATED_AT.toString(),
                11L,
                22L,
                new OrderStateSnapshot(List.of(order), 42L),
                List.of(account),
                List.of(book),
                List.of(new TopicPartitionOffset(
                        "order-commands", "order-command-consumer", 0, 7L)),
                List.of(
                        new ReplayCheckpoint("account-offset-seq", 0, 5L),
                        new ReplayCheckpoint("order-offset-seq", 0, 9L)));
    }

    static SnapshotPayload withoutBooks() {
        SnapshotPayload valid = validPayload();
        return new SnapshotPayload(valid.instanceId(), valid.createdAt(), valid.accountSeq(),
                valid.orderSeq(), valid.orderState(), valid.accounts(), List.of(),
                valid.inputOffsets(), valid.replayCheckpoints());
    }

    static SnapshotPayload withoutCheckpoints() {
        SnapshotPayload valid = validPayload();
        return new SnapshotPayload(valid.instanceId(), valid.createdAt(), valid.accountSeq(),
                valid.orderSeq(), valid.orderState(), valid.accounts(), valid.books(),
                valid.inputOffsets(), List.of());
    }
}
