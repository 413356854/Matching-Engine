package com.me.aeron;

import com.me.shared.gateway.AeronAction;
import com.me.shared.gateway.AeronOrderAck;
import com.me.shared.gateway.AeronOrderRequest;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import org.apache.fory.Fory;

/**
 * Aeron 消息的 Fory 实例工厂与类注册表。
 *
 * <p>Aeron 与 Kafka 使用两套独立的 Fory 注册表：Kafka 的 ID（{@code KafkaFory}）已经上线，
 * 这里只注册 WS 交易契约，避免两个协议互相牵制。ID 一旦上线必须保持稳定。
 *
 * <p>Fory 实例不是线程安全的；调用方必须为每个发布 / 消费线程创建独立实例。
 */
public final class AeronFory {

    private AeronFory() {
    }

    /** 创建一个独立的 Fory 实例；调用方必须保证同一实例不被并发使用。 */
    public static Fory newFory() {
        Fory fory = Fory.builder()
                .withXlang(false)
                .requireClassRegistration(true)
                .withRefTracking(false)
                .build();

        fory.register(AeronOrderRequest.class, 1);
        fory.register(AeronOrderAck.class, 2);
        fory.register(AeronAction.class, 3);
        fory.register(OrderSide.class, 4);
        fory.register(OrderType.class, 5);
        return fory;
    }
}
