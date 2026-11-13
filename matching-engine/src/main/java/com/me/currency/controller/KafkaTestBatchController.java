package com.me.currency.controller;

import com.me.currency.dto.KafkaTestBatchRequest;
import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.OrderCommandAssembler;
import com.me.currency.vo.KafkaTestBatchView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 批量下单测试接口（压测 / 外部批量接入）。
 *
 * <p>{@code POST /api/v1/currency/orders/kafka-test-batch}（兼容
 * {@code /kafkaTestBatch}）一次接收多条下单请求，逐单同步校验后由
 * {@link OrderCommandAssembler} 生成委托号并组装命令，再按顺序写入与单笔下单相同的
 * 进程内 Disruptor；后续聚合、Kafka 批量消息、委托保存、冻结与撮合链路完全一致。
 *
 * <p><b>请求不携带 orderId</b>：委托号全部由币币模块生成。
 *
 * <p>批量上限见 {@link #MAX_BATCH_ORDERS}；超过上限返回 {@code 400 BATCH_TOO_LARGE}。
 * 单条校验失败不会影响其它条目，{@code accepted} / {@code rejected} 分别计数，并在
 * {@code firstRejectReason} 返回第一条拒绝原因。Disruptor ring 满或发布失败时返回
 * {@code 503 ORDER_COMMAND_PUBLISH_FAILED}，已受理的条数仍会回传。
 */
@RestController
@RequestMapping("/api/v1/currency/orders")
public class KafkaTestBatchController {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestBatchController.class);

    /** 单次批量请求最多接收的订单数。 */
    static final int MAX_BATCH_ORDERS = 10_000;

    private final OrderCommandAssembler commandAssembler;
    private final OrderCommandPublisher orderCommandPublisher;

    public KafkaTestBatchController(OrderCommandAssembler commandAssembler,
                                    OrderCommandPublisher orderCommandPublisher) {
        this.commandAssembler = commandAssembler;
        this.orderCommandPublisher = orderCommandPublisher;
    }

    @PostMapping({"/kafka-test-batch", "/kafkaTestBatch"})
    public ResponseEntity<KafkaTestBatchView> kafkaTestBatch(
            @RequestBody(required = false) KafkaTestBatchRequest request) {
        if (request == null || request.orders().isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(KafkaTestBatchView.rejected("ORDERS_REQUIRED"));
        }
        if (request.orders().size() > MAX_BATCH_ORDERS) {
            return ResponseEntity.badRequest()
                    .body(KafkaTestBatchView.rejected("BATCH_TOO_LARGE"));
        }

        int accepted = 0;
        int rejected = 0;
        Long firstOrderId = null;
        Long lastOrderId = null;
        String firstRejectReason = null;

        for (PlaceOrderRequest order : request.orders()) {
            OrderCommandAssembler.Result result = commandAssembler.assemble(order);
            if (!result.accepted()) {
                rejected++;
                if (firstRejectReason == null) {
                    firstRejectReason = result.reason();
                }
                continue;
            }
            try {
                orderCommandPublisher.publish(result.command());
            } catch (RuntimeException e) {
                log.error("kafka test batch publish failed: accepted={}, rejected={}",
                        accepted, rejected, e);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(KafkaTestBatchView.publishFailed(
                                "ORDER_COMMAND_PUBLISH_FAILED", accepted, rejected));
            }
            accepted++;
            if (firstOrderId == null) {
                firstOrderId = result.orderId();
            }
            lastOrderId = result.orderId();
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(KafkaTestBatchView.accepted(
                        accepted, rejected, firstOrderId, lastOrderId, firstRejectReason));
    }
}
