package com.me.currency.controller;

import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.OrderCommandAssembler;
import com.me.currency.vo.PlaceOrderSubmissionView;
import com.me.shared.order.OrderCommand;
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
 * 币币模块下单接口：同步校验后只向 {@code me.order.commands-batch} 投递委托命令。
 *
 * <p><b>禁止直接调用委托模块</b>：本接口不保存委托、不冻结资金，也不 import 任何
 * {@code com.me.order} 类型；委托模块的 {@code OrderCommandListener} 消费命令，是系统唯一的
 * 下单入口。
 *
 * <p>{@code POST /api/v1/currency/orders} 成功返回 {@code 202 Accepted} 和命令回执
 * （requestId / orderId / symbol）。<b>委托号由币币模块生成，请求不接受客户端指定
 * {@code orderId}</b>。命令先非阻塞写入进程内 Disruptor，聚合线程把订单
 * 按 symbol 放入 {@code Map<String, List>}；任一 symbol 达到 2000 条或 50ms 窗口到达时，
 * 把 map 中所有 symbol 的订单分别组装成 {@code OrderCommandBatch} 发往 Kafka；202 只表示
 * 命令已进入 ring，不等待 broker ack。冻结失败、重复委托、撮合结果等由委托模块异步推进，
 * 不会通过本接口返回。
 *
 * <p>压测 / 外部批量接入使用 {@code POST /api/v1/currency/orders/kafka-test-batch}
 * （见 {@link KafkaTestBatchController}），批量请求同样不携带 orderId。
 */
@RestController
@RequestMapping("/api/v1/currency/orders")
public class OrderPlacementController {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacementController.class);

    private final OrderCommandAssembler commandAssembler;
    private final OrderCommandPublisher orderCommandPublisher;
    private final boolean orderPublishEnabled;

    public OrderPlacementController(
            OrderCommandAssembler commandAssembler,
            OrderCommandPublisher orderCommandPublisher,
            @Value("${app.currency.order-publish-enabled:true}") boolean orderPublishEnabled) {
        this.commandAssembler = commandAssembler;
        this.orderCommandPublisher = orderCommandPublisher;
        this.orderPublishEnabled = orderPublishEnabled;
    }

    @PostMapping
    public ResponseEntity<PlaceOrderSubmissionView> place(
            @RequestBody(required = false) PlaceOrderRequest request) {
        OrderCommandAssembler.Result result = commandAssembler.assemble(request);
        if (!result.accepted()) {
            return ResponseEntity.badRequest()
                    .body(PlaceOrderSubmissionView.rejected(result.requestId(), result.reason()));
        }

        OrderCommand command = result.command();
        if (orderPublishEnabled) {
            try {
                orderCommandPublisher.publish(command);
            } catch (RuntimeException e) {
                log.error("order command publish failed: requestId={}, orderId={}, symbol={}",
                        result.requestId(), result.orderId(), result.symbol(), e);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(PlaceOrderSubmissionView.rejected(
                                result.requestId(), result.orderId(), result.symbol(),
                                "ORDER_COMMAND_PUBLISH_FAILED"));
            }
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(PlaceOrderSubmissionView.accepted(
                        result.requestId(), result.orderId(), result.symbol()));
    }
}
