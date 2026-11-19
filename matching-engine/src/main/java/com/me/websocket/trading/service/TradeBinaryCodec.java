package com.me.websocket.trading.service;

import com.me.shared.gateway.AeronOrderAck;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import com.me.websocket.trading.dto.TradeClientMessage;
import com.me.websocket.trading.sbe.AckBatchEncoder;
import com.me.websocket.trading.sbe.AckTimeoutEncoder;
import com.me.websocket.trading.sbe.ErrorEncoder;
import com.me.websocket.trading.sbe.LoginAckEncoder;
import com.me.websocket.trading.sbe.LoginDecoder;
import com.me.websocket.trading.sbe.MessageHeaderDecoder;
import com.me.websocket.trading.sbe.MessageHeaderEncoder;
import com.me.websocket.trading.sbe.OrderDecoder;
import com.me.websocket.trading.sbe.PingDecoder;
import com.me.websocket.trading.sbe.PongEncoder;
import java.nio.ByteBuffer;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * /ws/trade 的 SBE 二进制协议编解码。
 *
 * <p>一个 WebSocket 二进制消息承载一条 SBE 消息（{@code messageHeader} + 消息体）。
 * 客户端消息：{@code Login(10)}、{@code Order(20)}、{@code Ping(42)}；服务端消息：
 * {@code LoginAck(11)}、{@code AckBatch(30)}、{@code Error(40)}、{@code AckTimeout(41)}、
 * {@code Pong(43)}。schema 见 {@code src/main/resources/sbe/trade-ws.xml}，由 Maven 在
 * {@code generate-sources} 阶段生成 codec。
 *
 * <p>服务端把同一连接、同一次 Aeron poll 内的多条 ack 编码为一个 {@code AckBatch}，
 * 用重复组承载，一条 WebSocket 帧最多 {@code ack-batch-size} 条；requestId / reason 为
 * UTF-8 变长字段。{@code Order} 中 int64 字段以 0 表示“未提供”，解码后还原为 null，
 * 交给币币模块复用 REST 的必填校验。
 *
 * <p>无状态：编码器按调用栈分配，可在 Tomcat 线程和多个 ack poller 线程间共享。
 */
public final class TradeBinaryCodec {

    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    /** groupSizeEncoding：blockLength + numInGroup。 */
    private static final int ACK_GROUP_HEADER_LENGTH = 4;
    /** AckBatch 每个元素的固定块：success(uint8) + orderId(int64)。 */
    private static final int ACK_FIXED_LENGTH = 9;
    /** varDataEncoding 的长度前缀（uint16）。 */
    private static final int VAR_DATA_HEADER_LENGTH = 2;
    /** UTF-8 每个 char 最多 3 字节（代理对平均 2 字节/char），用于编码前的容量估算。 */
    private static final int MAX_UTF8_BYTES_PER_CHAR = 3;

    /** 解码一条客户端消息；格式非法 / 未知模板 / 未知枚举值时抛出 IllegalArgumentException。 */
    public TradeClientMessage read(ByteBuffer payload) {
        if (payload == null || payload.remaining() < HEADER_LENGTH) {
            throw new IllegalArgumentException("binary message too short");
        }
        // 复制到 byte[]：Tomcat 可能传入只读 / 直接 ByteBuffer，Agrona 的
        // UnsafeBuffer(ByteBuffer) 对只读堆缓冲不可用；单帧只有几十字节，复制成本可忽略。
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, 0);
        int blockLength = header.blockLength();
        int version = header.version();

        return switch (header.templateId()) {
            case LoginDecoder.TEMPLATE_ID -> readLogin(buffer, blockLength, version);
            case OrderDecoder.TEMPLATE_ID -> readOrder(buffer, blockLength, version);
            case PingDecoder.TEMPLATE_ID -> new TradeClientMessage(
                    "ping", null, null, null, null, null, null, null, null, null);
            default -> throw new IllegalArgumentException(
                    "unknown sbe template: " + header.templateId());
        };
    }

    private static TradeClientMessage readLogin(DirectBuffer buffer, int blockLength, int version) {
        LoginDecoder decoder = new LoginDecoder();
        decoder.wrap(buffer, HEADER_LENGTH, blockLength, version);
        return new TradeClientMessage(
                "login", decoder.userId(), null, null, null, null, null, null, null, null);
    }

    private static TradeClientMessage readOrder(DirectBuffer buffer, int blockLength, int version) {
        OrderDecoder decoder = new OrderDecoder();
        decoder.wrap(buffer, HEADER_LENGTH, blockLength, version);

        String action = switch (decoder.action()) {
            case PLACE -> "placeOrder";
            case CANCEL -> "cancelOrder";
            default -> null;
        };
        OrderSide side = switch (decoder.side()) {
            case BUY -> OrderSide.BUY;
            case SELL -> OrderSide.SELL;
            default -> null;
        };
        OrderType type = switch (decoder.orderType()) {
            case LIMIT -> OrderType.LIMIT;
            case MARKET -> OrderType.MARKET;
            default -> null;
        };
        return new TradeClientMessage(
                action,
                null,
                emptyToNull(decoder.requestId()),
                emptyToNull(decoder.symbol()),
                side,
                type,
                zeroToNull(decoder.price()),
                zeroToNull(decoder.quantity()),
                zeroToNull(decoder.amount()),
                zeroToNull(decoder.orderId()));
    }

    /** 登录成功回执。 */
    public ByteBuffer writeLoginAck(long userId, boolean success, String reason) {
        ByteBuffer buffer = allocate(HEADER_LENGTH + LoginAckEncoder.BLOCK_LENGTH
                + VAR_DATA_HEADER_LENGTH + varDataMaxBytes(reason));
        MutableDirectBuffer direct = new UnsafeBuffer(buffer);
        LoginAckEncoder encoder = new LoginAckEncoder();
        encoder.wrapAndApplyHeader(direct, 0, new MessageHeaderEncoder());
        encoder.userId(userId);
        encoder.success((short) (success ? 1 : 0));
        encoder.reason(reason == null ? "" : reason);
        return finish(buffer, HEADER_LENGTH + encoder.encodedLength());
    }

    /** 心跳响应。 */
    public ByteBuffer writePong() {
        ByteBuffer buffer = allocate(HEADER_LENGTH + PongEncoder.BLOCK_LENGTH);
        MutableDirectBuffer direct = new UnsafeBuffer(buffer);
        PongEncoder encoder = new PongEncoder();
        encoder.wrapAndApplyHeader(direct, 0, new MessageHeaderEncoder());
        return finish(buffer, HEADER_LENGTH + encoder.encodedLength());
    }

    /** WS 层 / 传输层错误。 */
    public ByteBuffer writeError(String requestId, String reason) {
        String rid = requestId == null ? "" : requestId;
        String why = reason == null ? "" : reason;
        ByteBuffer buffer = allocate(HEADER_LENGTH + ErrorEncoder.BLOCK_LENGTH
                + 2 * VAR_DATA_HEADER_LENGTH + varDataMaxBytes(rid) + varDataMaxBytes(why));
        MutableDirectBuffer direct = new UnsafeBuffer(buffer);
        ErrorEncoder encoder = new ErrorEncoder();
        encoder.wrapAndApplyHeader(direct, 0, new MessageHeaderEncoder());
        encoder.requestId(rid);
        encoder.reason(why);
        return finish(buffer, HEADER_LENGTH + encoder.encodedLength());
    }

    /** 等待 ingress ack 超时。 */
    public ByteBuffer writeAckTimeout(String requestId) {
        String rid = requestId == null ? "" : requestId;
        ByteBuffer buffer = allocate(HEADER_LENGTH + AckTimeoutEncoder.BLOCK_LENGTH
                + VAR_DATA_HEADER_LENGTH + varDataMaxBytes(rid));
        MutableDirectBuffer direct = new UnsafeBuffer(buffer);
        AckTimeoutEncoder encoder = new AckTimeoutEncoder();
        encoder.wrapAndApplyHeader(direct, 0, new MessageHeaderEncoder());
        encoder.requestId(rid);
        return finish(buffer, HEADER_LENGTH + encoder.encodedLength());
    }

    /**
     * 把一批 ack 编码成一个 {@code AckBatch} 帧；调用方负责按 {@link #ackBatchEnd} 切片，
     * 保证帧不超过 {@code app.websocket.trading.max-message-bytes}。
     */
    public ByteBuffer writeAckBatch(List<AeronOrderAck> acks) {
        int capacity = ackBatchBaseBytes();
        for (int i = 0; i < acks.size(); i++) {
            capacity += ackEntryMaxBytes(acks.get(i));
        }
        ByteBuffer buffer = allocate(capacity);
        MutableDirectBuffer direct = new UnsafeBuffer(buffer);
        AckBatchEncoder encoder = new AckBatchEncoder();
        encoder.wrapAndApplyHeader(direct, 0, new MessageHeaderEncoder());
        AckBatchEncoder.AcksEncoder group = encoder.acksCount(acks.size());
        for (int i = 0; i < acks.size(); i++) {
            AeronOrderAck ack = acks.get(i);
            group.next();
            group.success((short) (ack.success() ? 1 : 0));
            group.orderId(ack.orderId() == null ? 0L : ack.orderId());
            group.requestId(ack.requestId());
            group.reason(ack.reason());
        }
        return finish(buffer, HEADER_LENGTH + encoder.encodedLength());
    }

    /**
     * 从 {@code start} 开始，按条数上限和字节上限（估算值）计算本帧的结束下标（不含）。
     * 至少包含一条，保证超长 requestId 也能发出。
     */
    public int ackBatchEnd(List<AeronOrderAck> acks, int start, int maxCount, int maxBytes) {
        int end = start;
        int bytes = ackBatchBaseBytes();
        while (end < acks.size() && end - start < maxCount) {
            int entryBytes = ackEntryMaxBytes(acks.get(end));
            if (end > start && bytes + entryBytes > maxBytes) {
                break;
            }
            bytes += entryBytes;
            end++;
        }
        return end;
    }

    /** 一帧 AckBatch 的固定开销：消息头 + 重复组头。 */
    public int ackBatchBaseBytes() {
        return HEADER_LENGTH + ACK_GROUP_HEADER_LENGTH;
    }

    /** 一条 ack 的编码上限（按 UTF-8 最多 3 字节/char 估算）。 */
    public int ackEntryMaxBytes(AeronOrderAck ack) {
        return ACK_FIXED_LENGTH + 2 * VAR_DATA_HEADER_LENGTH
                + varDataMaxBytes(ack.requestId()) + varDataMaxBytes(ack.reason());
    }

    private static int varDataMaxBytes(String value) {
        return value == null ? 0 : value.length() * MAX_UTF8_BYTES_PER_CHAR;
    }

    private static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocate(Math.max(capacity, HEADER_LENGTH));
    }

    private static ByteBuffer finish(ByteBuffer buffer, int length) {
        buffer.position(0);
        buffer.limit(length);
        return buffer;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    private static Long zeroToNull(long value) {
        return value == 0 ? null : value;
    }
}
