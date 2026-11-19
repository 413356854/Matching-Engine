package com.me.currency.controller;

import com.me.currency.dto.CancelOrderRequest;
import com.me.currency.mq.OrderCancelCommandPublisher;
import com.me.currency.service.OrderCancelAssembler;
import com.me.currency.vo.CancelOrderSubmissionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 币币模块撤单接口：同步校验后只向 {@code me.order.cancels-batch} 投递撤单命令。
 *
 * <p><b>禁止直接调用委托模块</b>：本接口不查委托、不修改委托状态、不解冻资金，也不 import
 * 任何 {@code com.me.order} 类型；委托模块的 {@code OrderCancelListener} 消费命令，是系统
 * 唯一的撤单入口。
 *
 * <p>{@code POST /api/v1/currency/orders/cancel} 成功返回 {@code 202 Accepted} 和命令回执
 * （requestId / orderId / symbol）。同步校验由 {@link OrderCancelAssembler} 完成，命令先
 * 非阻塞写入<b>独立的撤单 Disruptor</b>，再由聚合线程按「单 symbol 1000 条或 20ms」把 map
 * 中所有 symbol 的撤单分别封装成 {@code OrderCancelBatch} 发往 Kafka；202 只表示命令已
 * 进入 ring，不等待 broker ack。委托是否可撤、解冻结果等由委托模块异步推进，不会通过
 * 本接口返回。
 */
@RestController
@RequestMapping("/api/v1/currency/orders")
public class OrderCancelController {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelController.class);

    private final OrderCancelAssembler cancelAssembler;
    private final OrderCancelCommandPublisher cancelCommandPublisher;
    private final boolean cancelPublishEnabled;

    public OrderCancelController(
            OrderCancelAssembler cancelAssembler,
            OrderCancelCommandPublisher cancelCommandPublisher,
            @Value("${app.currency.cancel-publish-enabled:true}") boolean cancelPublishEnabled) {
        this.cancelAssembler = cancelAssembler;
        this.cancelCommandPublisher = cancelCommandPublisher;
        this.cancelPublishEnabled = cancelPublishEnabled;
    }

    @PostMapping("/cancel")
    public ResponseEntity<CancelOrderSubmissionView> cancel(
            @RequestBody(required = false) CancelOrderRequest request) {
        OrderCancelAssembler.Result result = cancelAssembler.assemble(request);
        if (!result.accepted()) {
            return ResponseEntity.badRequest()
                    .body(CancelOrderSubmissionView.rejected(
                            result.requestId(), result.reason()));
        }

        if (cancelPublishEnabled) {
            try {
                cancelCommandPublisher.publish(result.command());
            } catch (RuntimeException e) {
                log.error("order cancel command publish failed: requestId={}, orderId={}, symbol={}",
                        result.requestId(), result.orderId(), result.symbol(), e);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(CancelOrderSubmissionView.rejected(
                                result.requestId(), result.orderId(), result.symbol(),
                                "CANCEL_COMMAND_PUBLISH_FAILED"));
            }
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(CancelOrderSubmissionView.accepted(
                        result.requestId(), result.orderId(), result.symbol()));
    }
}
