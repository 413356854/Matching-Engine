package com.me.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.me.account.service.impl.AccountServiceImpl;
import com.me.account.vo.AccountView;
import com.me.matching.entity.BookOrder;
import com.me.matching.service.MatchingEngine;
import com.me.matching.vo.DepthSnapshot;
import com.me.order.service.impl.OrderServiceImpl;
import com.me.order.vo.OrderView;
import com.me.shared.order.OrderStatus;
import com.me.snapshot.dto.SnapshotPayload;
import org.junit.jupiter.api.Test;

class SnapshotStateRestoreTest {

    @Test
    void restoresAccountsOrdersAndOrderBook() {
        SnapshotPayload payload = SnapshotTestData.validPayload();
        AccountServiceImpl accountService = new AccountServiceImpl();
        OrderServiceImpl orderService = new OrderServiceImpl();
        MatchingEngine matchingEngine = new MatchingEngine();

        accountService.restoreAccounts(payload.accounts());
        orderService.restoreState(payload.orderState());
        matchingEngine.restore(payload.books());

        AccountView account = accountService.view(1L, "USDT");
        assertEquals(900L, account.available());
        assertEquals(100L, account.frozen());
        assertEquals(3L, account.seq());

        OrderView order = orderService.view(1L);
        assertNotNull(order);
        assertEquals(OrderStatus.PAID, order.status());
        assertEquals(2L, order.seq());
        assertEquals(42L, orderService.snapshotState().tradeSettlementIdGenerator());

        BookOrder bookOrder = matchingEngine.book("BTC-USDT").find(1L);
        assertNotNull(bookOrder);
        assertEquals(10L, bookOrder.remainingQuantity());

        DepthSnapshot depth = matchingEngine.depth("BTC-USDT", 5);
        assertEquals(10L, depth.bestBid());
        assertEquals(1, depth.bids().size());
        assertEquals(10L, depth.bids().get(0).quantity());
        assertEquals(1, depth.bids().get(0).orderCount());
    }

    @Test
    void restoreReplacesPreviousState() {
        SnapshotPayload payload = SnapshotTestData.validPayload();
        AccountServiceImpl accountService = new AccountServiceImpl();
        OrderServiceImpl orderService = new OrderServiceImpl();
        MatchingEngine matchingEngine = new MatchingEngine();

        accountService.restoreAccounts(payload.accounts());
        orderService.restoreState(payload.orderState());
        matchingEngine.restore(payload.books());

        accountService.restoreAccounts(java.util.List.of());
        orderService.restoreState(new com.me.order.dto.OrderStateSnapshot(java.util.List.of(), 99L));
        matchingEngine.restore(java.util.List.of());

        assertEquals(0L, accountService.view(1L, "USDT").available());
        assertEquals(null, orderService.view(1L));
        assertEquals(null, matchingEngine.book("BTC-USDT"));
        assertEquals(99L, orderService.snapshotState().tradeSettlementIdGenerator());
    }
}
