package com.me.currency.ingress.aeron;

import com.me.currency.dto.CancelOrderRequest;
import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.ingress.IngressSubmitResult;
import com.me.currency.mq.OrderCancelCommandPublisher;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.OrderCancelAssembler;
import com.me.currency.service.OrderCommandAssembler;
import com.me.kafka.FatalState;
import com.me.shared.gateway.AeronAction;
import com.me.shared.gateway.AeronOrderAck;
import com.me.shared.gateway.AeronOrderRequest;
import com.me.snapshot.recovery.RecoveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aeron 下单/撤单请求的业务处理：校验、组装并写入现有 ingress ring。
 *
 * <p>与 HTTP 入口共用 {@link OrderCommandAssembler} / {@link OrderCancelAssembler}，
 * 因此错误码、精度校验和委托号生成规则完全一致。成功只表示命令已进入 ring；
 * 返回的 ack 不等待 Kafka / 冻结 / 撮合。
 *
 * <p><b>异常丢弃</b>：ring 满、ring 未启动或处理异常时不重试，返回失败 ack 后丢弃；
 * ack 本身也发送失败时只记录日志。客户端超时后应查询委托，不要自动重试。
 */
public final class AeronRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(AeronRequestProcessor.class);

    private static final int MAX_REQUEST_ID_LENGTH = 128;

    private final OrderCommandAssembler orderCommandAssembler;
    private final OrderCancelAssembler orderCancelAssembler;
    private final OrderCommandPublisher orderCommandPublisher;
    private final OrderCancelCommandPublisher orderCancelCommandPublisher;
    private final FatalState fatalState;
    private final RecoveryState recoveryState;

    public AeronRequestProcessor(
            OrderCommandAssembler orderCommandAssembler,
            OrderCancelAssembler orderCancelAssembler,
            OrderCommandPublisher orderCommandPublisher,
            OrderCancelCommandPublisher orderCancelCommandPublisher,
            FatalState fatalState,
            RecoveryState recoveryState) {
        this.orderCommandAssembler = orderCommandAssembler;
        this.orderCancelAssembler = orderCancelAssembler;
        this.orderCommandPublisher = orderCommandPublisher;
        this.orderCancelCommandPublisher = orderCancelCommandPublisher;
        this.fatalState = fatalState;
        this.recoveryState = recoveryState;
    }

    /**
     * 处理一条请求；返回 {@code null} 表示请求无法关联（例如 requestId 为空），不发送 ack。
     */
    public AeronOrderAck process(AeronOrderRequest request) {
        if (request == null) {
            return null;
        }
        String requestId = normalizeRequestId(request.requestId());
        if (requestId == null) {
            log.error("aeron order request dropped: requestId is blank, connectionId={}",
                    request.connectionId());
            return null;
        }
        if (request.version() != AeronOrderRequest.CURRENT_VERSION) {
            return failure(request, requestId, "UNSUPPORTED_VERSION");
        }
        if (request.action() == null) {
            return failure(request, requestId, "ACTION_REQUIRED");
        }
        if (request.userId() <= 0) {
            return failure(request, requestId, "USER_ID_INVALID");
        }
        if (requestId.length() > MAX_REQUEST_ID_LENGTH) {
            return failure(request, requestId, "REQUEST_ID_INVALID");
        }
        if (fatalState.isFatal()) {
            return failure(request, requestId, "ENGINE_UNAVAILABLE");
        }
        if (!recoveryState.isReady()) {
            return failure(request, requestId, "RECOVERY_IN_PROGRESS");
        }

        try {
            return switch (request.action()) {
                case PLACE -> processPlace(request, requestId);
                case CANCEL -> processCancel(request, requestId);
            };
        } catch (RuntimeException e) {
            log.error("aeron order request failed: action={}, requestId={}, connectionId={}",
                    request.action(), requestId, request.connectionId(), e);
            return failure(request, requestId, "INTERNAL_ERROR");
        }
    }

    private AeronOrderAck processPlace(AeronOrderRequest request, String requestId) {
        PlaceOrderRequest place = new PlaceOrderRequest(
                requestId,
                request.userId(),
                request.symbol(),
                request.side(),
                request.orderType(),
                request.price(),
                request.quantity(),
                request.amount());
        OrderCommandAssembler.Result result = orderCommandAssembler.assemble(place);
        if (!result.accepted()) {
            return failure(request, requestId, result.reason());
        }
        return afterSubmit(request, requestId, result.orderId(), result.symbol(),
                orderCommandPublisher.tryPublish(result.command()));
    }

    private AeronOrderAck processCancel(AeronOrderRequest request, String requestId) {
        CancelOrderRequest cancel = new CancelOrderRequest(
                requestId, request.orderId(), request.symbol(), request.userId());
        OrderCancelAssembler.Result result = orderCancelAssembler.assemble(cancel);
        if (!result.accepted()) {
            return failure(request, requestId, result.reason());
        }
        return afterSubmit(request, requestId, result.orderId(), result.symbol(),
                orderCancelCommandPublisher.tryPublish(result.command()));
    }

    private AeronOrderAck afterSubmit(AeronOrderRequest request, String requestId,
                                      Long orderId, String symbol,
                                      IngressSubmitResult submitResult) {
        return switch (submitResult) {
            case ACCEPTED -> new AeronOrderAck(
                    AeronOrderAck.CURRENT_VERSION, requestId, request.connectionId(),
                    true, null, orderId, symbol);
            case FULL -> {
                log.error("aeron order dropped, ingress ring full: action={}, requestId={}, "
                                + "orderId={}, symbol={}",
                        request.action(), requestId, orderId, symbol);
                yield failure(request, requestId, "SERVER_BUSY");
            }
            case NOT_RUNNING -> {
                log.error("aeron order dropped, ingress ring not running: action={}, "
                                + "requestId={}, orderId={}, symbol={}",
                        request.action(), requestId, orderId, symbol);
                yield failure(request, requestId, "ENGINE_UNAVAILABLE");
            }
            case ERROR -> {
                log.error("aeron order dropped, ingress enqueue error: action={}, requestId={}, "
                                + "orderId={}, symbol={}",
                        request.action(), requestId, orderId, symbol);
                yield failure(request, requestId, "INTERNAL_ERROR");
            }
        };
    }

    private static AeronOrderAck failure(AeronOrderRequest request, String requestId,
                                         String reason) {
        return new AeronOrderAck(
                AeronOrderAck.CURRENT_VERSION, requestId, request.connectionId(),
                false, reason, null, request.symbol());
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null) {
            return null;
        }
        String normalized = requestId.strip();
        return normalized.isEmpty() ? null : normalized;
    }
}
