package com.me.aeron;

import com.me.shared.gateway.AeronOrderAck;
import com.me.shared.gateway.AeronOrderRequest;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.apache.fory.Fory;
import org.springframework.stereotype.Component;

/**
 * Aeron 消息编解码：4 字节大端长度前缀 + Fory 负载。
 *
 * <p>发布线程和消费线程各自持有独立的 {@link Fory}（{@link ThreadLocal}），因为 Fory
 * 实例不是线程安全的。消息长度同时受 {@code app.aeron.max-message-bytes} 限制，解码时
 * 会校验前缀长度与实际 fragment 长度一致，避免损坏消息导致越界读取。
 */
@Component
public class AeronCodec {

    private final ThreadLocal<Fory> fory = ThreadLocal.withInitial(AeronFory::newFory);
    private final int maxMessageBytes;

    public AeronCodec(AeronProperties properties) {
        this.maxMessageBytes = properties.maxMessageBytes();
    }

    /** 编码下单/撤单请求，返回写入的字节数。 */
    public int encodeRequest(AeronOrderRequest request, MutableDirectBuffer buffer, int offset) {
        return encode(request, buffer, offset);
    }

    /** 解码下单/撤单请求；长度或格式非法时抛出 {@link IllegalArgumentException}。 */
    public AeronOrderRequest decodeRequest(DirectBuffer buffer, int offset, int length) {
        return decode(buffer, offset, length, AeronOrderRequest.class);
    }

    /** 编码 ingress ack，返回写入的字节数。 */
    public int encodeAck(AeronOrderAck ack, MutableDirectBuffer buffer, int offset) {
        return encode(ack, buffer, offset);
    }

    /** 解码 ingress ack；长度或格式非法时抛出 {@link IllegalArgumentException}。 */
    public AeronOrderAck decodeAck(DirectBuffer buffer, int offset, int length) {
        return decode(buffer, offset, length, AeronOrderAck.class);
    }

    private int encode(Object value, MutableDirectBuffer buffer, int offset) {
        byte[] payload = fory.get().serialize(value);
        if (payload.length > maxMessageBytes) {
            throw new IllegalArgumentException(
                    "aeron message exceeds max size: " + payload.length + " > " + maxMessageBytes);
        }
        buffer.putInt(offset, payload.length, ByteOrder.BIG_ENDIAN);
        buffer.putBytes(offset + Integer.BYTES, payload);
        return Integer.BYTES + payload.length;
    }

    private <T> T decode(DirectBuffer buffer, int offset, int length, Class<T> type) {
        if (length < Integer.BYTES) {
            throw new IllegalArgumentException("aeron message too short: " + length);
        }
        int payloadLength = buffer.getInt(offset, ByteOrder.BIG_ENDIAN);
        if (payloadLength <= 0 || payloadLength > maxMessageBytes) {
            throw new IllegalArgumentException("aeron payload length invalid: " + payloadLength);
        }
        if (payloadLength != length - Integer.BYTES) {
            throw new IllegalArgumentException("aeron payload length mismatch: prefix="
                    + payloadLength + ", fragment=" + length);
        }
        byte[] payload = new byte[payloadLength];
        buffer.getBytes(offset + Integer.BYTES, payload);
        return fory.get().deserialize(payload, type);
    }
}
