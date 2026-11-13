package com.me.kafka;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.event.FundsFlowBatch;
import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.FundsOp;
import com.me.shared.event.MarketDataBatch;
import com.me.shared.event.MarketDataEvent;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCancelCommand;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import org.apache.fory.Fory;

/**
 * Kafka value 的 Fory 实例工厂与类注册表。
 *
 * <p><b>每个 producer / consumer 使用独立 Fory 实例</b>，不再共享 ThreadSafeFory 池：
 * <ul>
 *   <li>producer 的序列化在调用 send 的线程上执行，KafkaRestPublisher 的 producer 池
 *       保证同一 producer 同一时刻只借给一个线程；</li>
 *   <li>每个 consumer runner 只有一个消费线程，反序列化也在该线程执行；</li>
 * </ul>
 * 因此普通 {@link Fory}（非线程安全）足够，且避免了共享池的竞争。
 *
 * <p>类在创建时显式注册，开启 {@code requireClassRegistration=true}，避免反序列化任意类。
 * 新增 Kafka 消息类型时必须在这里注册，并保持 ID 稳定。
 */
public final class KafkaFory {

    private KafkaFory() {
    }

    /** 创建一个独立的 Fory 实例；调用方必须保证同一实例不被并发使用。 */
    public static Fory newFory() {
        Fory fory = Fory.builder()
                .withXlang(false)
                .requireClassRegistration(true)
                .withRefTracking(false)
                .build();

        // 消息契约类；ID 一旦上线必须保持稳定。
        fory.register(OrderCommand.class, 100);
        fory.register(OrderSide.class, 101);
        fory.register(OrderType.class, 102);
        fory.register(OrderStatus.class, 103);
        fory.register(OrderPlacedEvent.class, 104);
        fory.register(OrderCommandBatch.class, 105);
        fory.register(OrderCancelCommand.class, 106);
        fory.register(OrderCancelBatch.class, 107);

        fory.register(FundsCommand.class, 110);
        fory.register(FundsLogType.class, 111);
        fory.register(FundsOp.class, 112);
        fory.register(OrderFreezeCommandsBatch.class, 113);

        fory.register(OrderFreezeResultEvent.class, 120);
        fory.register(OrderFreezeResultBatch.class, 121);

        fory.register(TradeSettlementCommand.class, 130);
        fory.register(TradeSettlementCommand.SettlementLeg.class, 131);
        fory.register(TradeSettlementBatch.class, 132);

        fory.register(MarketDataEvent.class, 140);
        fory.register(MarketDataBatch.class, 141);

        fory.register(FundsFlowEvent.class, 150);
        fory.register(FundsFlowBatch.class, 151);
        return fory;
    }
}
