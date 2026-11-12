package com.me.currency.service;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 下单接口的委托号生成器：毫秒时间戳左移再加序号，单进程内递增且不重复。
 *
 * <p>委托仍保存在委托模块的内存中，进程重启后全部丢失；跨实例部署时需要换成全局 ID 方案。
 */
@Component
public class OrderIdGenerator {

    private final AtomicLong sequence = new AtomicLong(System.currentTimeMillis() * 1000L);

    public long nextId() {
        return sequence.incrementAndGet();
    }
}
