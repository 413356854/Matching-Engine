package com.me.snapshot;

import com.me.account.vo.AccountView;
import com.me.matching.vo.BookOrderSnapshot;
import com.me.matching.vo.OrderBookSnapshot;
import com.me.matching.vo.PriceLevelSnapshot;
import com.me.order.dto.OrderSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import com.me.snapshot.dto.ReplayCheckpoint;
import com.me.snapshot.dto.SnapshotFile;
import com.me.snapshot.dto.SnapshotPayload;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 快照文件加载与结构 / 一致性校验。
 *
 * <p>只负责“文件本身是否可信”：格式版本、SHA-256、账户 / 委托 / 订单簿之间的引用关系、
 * 冻结金额不变量。Kafka topic 分区数、消费组名称、broker committed offset 等运行期
 * 一致性由恢复服务在启动时继续校验。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class SnapshotLoader {

    private static final Logger log = LoggerFactory.getLogger(SnapshotLoader.class);

    /** 加载并校验快照；任何问题都抛出 {@link SnapshotRecoveryException}。 */
    public LoadedSnapshot load(Path path) {
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized)) {
            throw new SnapshotRecoveryException("snapshot file does not exist: " + normalized);
        }
        SnapshotFile file = SnapshotCodec.read(normalized);
        if (file.formatVersion() != SnapshotFile.CURRENT_FORMAT_VERSION) {
            throw new SnapshotRecoveryException("unsupported snapshot format version "
                    + file.formatVersion() + " (expected " + SnapshotFile.CURRENT_FORMAT_VERSION
                    + "): " + normalized);
        }
        SnapshotCodec.verifyChecksum(file, normalized);
        validatePayload(file.payload(), normalized);
        return new LoadedSnapshot(normalized, file);
    }

    /**
     * 快照整体结构与语义校验。
     *
     * <p>先检查必要字段齐全（v2 必须带 replayCheckpoints），再分别校验账户 / 委托 /
     * 订单簿，最后做跨结构交叉校验和冻结金额不变量。校验顺序从便宜到昂贵，
     * 任一失败都带文件路径和具体记录标识，便于运维定位坏文件。
     */
    private void validatePayload(SnapshotPayload payload, Path path) {
        if (payload == null) {
            throw fail(path, "payload is missing");
        }
        if (payload.instanceId() == null || payload.instanceId().isBlank()) {
            throw fail(path, "instanceId is missing");
        }
        parseInstant(payload.createdAt(), path, "createdAt");
        if (payload.accountSeq() < 0 || payload.orderSeq() < 0) {
            throw fail(path, "seq watermark is negative: accountSeq=" + payload.accountSeq()
                    + ", orderSeq=" + payload.orderSeq());
        }
        if (payload.orderState() == null) {
            throw fail(path, "orderState is missing");
        }
        if (payload.orderState().orders() == null) {
            throw fail(path, "orderState.orders is missing");
        }
        if (payload.orderState().tradeSettlementIdGenerator() < 1) {
            throw fail(path, "tradeSettlementIdGenerator must be positive: "
                    + payload.orderState().tradeSettlementIdGenerator());
        }
        if (payload.accounts() == null) {
            throw fail(path, "accounts is missing");
        }
        if (payload.books() == null) {
            throw fail(path, "books is missing");
        }
        if (payload.inputOffsets() == null || payload.inputOffsets().isEmpty()) {
            throw fail(path, "inputOffsets is missing or empty");
        }
        if (payload.replayCheckpoints() == null || payload.replayCheckpoints().isEmpty()) {
            throw fail(path, "replayCheckpoints is missing or empty "
                    + "(snapshot was written with an older format)");
        }

        validateAccounts(payload.accounts(), path);
        Map<Long, OrderSnapshot> orderById = validateOrders(payload.orderState(), path);
        Map<String, OrderBookSnapshot> bookBySymbol = validateBooks(payload.books(), path);
        validateCrossReferences(payload, orderById, bookBySymbol, path);
        validateInputOffsets(payload.inputOffsets(), path);
        validateReplayCheckpoints(payload.replayCheckpoints(), path);

        try {
            SnapshotInvariantValidator.validate(payload.accounts(), payload.orderState());
        } catch (IllegalStateException e) {
            throw fail(path, e.getMessage());
        }
    }

    /**
     * 账户记录校验：userId/currency 合法、余额和 seq 非负、同一 (userId, currency)
     * 只能出现一次。重复账户在恢复时会被覆盖，必须提前拒绝。
     */
    private void validateAccounts(List<AccountView> accounts, Path path) {
        Set<String> keys = new HashSet<>();
        for (AccountView account : accounts) {
            if (account == null) {
                throw fail(path, "accounts contains null");
            }
            if (account.userId() <= 0) {
                throw fail(path, "account userId must be positive: " + account.userId());
            }
            if (account.currency() == null || account.currency().isBlank()) {
                throw fail(path, "account currency is missing for userId " + account.userId());
            }
            if (account.available() < 0 || account.frozen() < 0) {
                throw fail(path, "account balance is negative: userId=" + account.userId()
                        + ", currency=" + account.currency() + ", available="
                        + account.available() + ", frozen=" + account.frozen());
            }
            if (account.seq() < 0) {
                throw fail(path, "account seq is negative: userId=" + account.userId()
                        + ", currency=" + account.currency() + ", seq=" + account.seq());
            }
            String key = account.userId() + "|" + account.currency().strip().toUpperCase();
            if (!keys.add(key)) {
                throw fail(path, "duplicate account: " + key);
            }
        }
    }

    /**
     * 委托记录校验。
     *
     * <p>除了字段合法性，这里还强制快照的静止语义：不允许存在 {@code ACCEPTED} 委托。
     * ACCEPTED 表示冻结回执还没处理完，说明内部 topic 尚未排空，快照不是一致切点。
     * 返回 orderId → snapshot 索引，供订单簿交叉校验使用。
     */
    private Map<Long, OrderSnapshot> validateOrders(OrderStateSnapshot orderState, Path path) {
        Map<Long, OrderSnapshot> orderById = new HashMap<>(orderState.orders().size() * 2);
        for (OrderSnapshot order : orderState.orders()) {
            if (order == null) {
                throw fail(path, "orders contains null");
            }
            if (order.orderId() <= 0) {
                throw fail(path, "orderId must be positive: " + order.orderId());
            }
            if (orderById.putIfAbsent(order.orderId(), order) != null) {
                throw fail(path, "duplicate orderId: " + order.orderId());
            }
            if (order.userId() <= 0) {
                throw fail(path, "order userId must be positive: " + order.orderId());
            }
            if (order.symbol() == null || order.symbol().isBlank()) {
                throw fail(path, "order symbol is missing: " + order.orderId());
            }
            if (order.requestId() == null || order.requestId().isBlank()) {
                throw fail(path, "order requestId is missing: " + order.orderId());
            }
            if (order.side() == null || order.type() == null || order.status() == null) {
                throw fail(path, "order side/type/status is missing: " + order.orderId());
            }
            if (order.quantity() <= 0) {
                throw fail(path, "order quantity must be positive: " + order.orderId());
            }
            if (order.filledQuantity() < 0 || order.filledQuantity() > order.quantity()) {
                throw fail(path, "order filledQuantity out of range: " + order.orderId()
                        + ", filled=" + order.filledQuantity() + ", quantity=" + order.quantity());
            }
            if (order.frozenAmount() < 0 || order.settledQuoteAmount() < 0) {
                throw fail(path, "order frozen amount is negative: " + order.orderId());
            }
            if (order.type() == OrderType.LIMIT && order.price() <= 0) {
                throw fail(path, "limit order price must be positive: " + order.orderId());
            }
            if (order.type() == OrderType.MARKET && order.price() != 0) {
                throw fail(path, "market order price must be zero: " + order.orderId());
            }
            if (order.priceScale() < 0 || order.quantityScale() < 0 || order.quoteScale() < 0) {
                throw fail(path, "order scale is negative: " + order.orderId());
            }
            if (order.status() == OrderStatus.ACCEPTED) {
                throw fail(path, "order is still ACCEPTED; snapshot is not quiescent: "
                        + order.orderId());
            }
            parseInstant(order.createdAt(), path, "order.createdAt=" + order.orderId());
            if (order.finalAt() != null) {
                parseInstant(order.finalAt(), path, "order.finalAt=" + order.orderId());
            }
        }
        return orderById;
    }

    /**
     * 订单簿结构校验。
     *
     * <p>逐项检查：
     * <ul>
     *   <li>symbol 唯一、价格档位非空且价格为正；</li>
     *   <li>挂单必须是 LIMIT 且价格等于档位价（市价单按 IOC 不挂单，不会出现在快照里）；</li>
     *   <li>挂单方向与所在集合一致（bids 为 BUY，asks 为 SELL）；</li>
     *   <li>remainingQuantity ∈ (0, quantity]；</li>
     *   <li>所有挂单都已在 seenOrderIds 中（去重集合是撮合幂等的基础）；</li>
     *   <li>nextTradeId 为正。</li>
     * </ul>
     */
    private Map<String, OrderBookSnapshot> validateBooks(List<OrderBookSnapshot> books, Path path) {
        Map<String, OrderBookSnapshot> bookBySymbol = new HashMap<>(books.size() * 2);
        for (OrderBookSnapshot book : books) {
            if (book == null) {
                throw fail(path, "books contains null");
            }
            if (book.symbol() == null || book.symbol().isBlank()) {
                throw fail(path, "book symbol is missing");
            }
            if (bookBySymbol.putIfAbsent(book.symbol(), book) != null) {
                throw fail(path, "duplicate book symbol: " + book.symbol());
            }
            if (book.bids() == null || book.asks() == null) {
                throw fail(path, "book levels are missing: " + book.symbol());
            }
            if (book.seenOrderIds() == null) {
                throw fail(path, "book seenOrderIds is missing: " + book.symbol());
            }
            if (book.nextTradeId() < 1) {
                throw fail(path, "book nextTradeId must be positive: " + book.symbol()
                        + ", nextTradeId=" + book.nextTradeId());
            }
            Set<Long> seen = validateSeenOrderIds(book, path);
            validateLevels(book, book.bids(), OrderSide.BUY, seen, path);
            validateLevels(book, book.asks(), OrderSide.SELL, seen, path);
        }
        return bookBySymbol;
    }

    private Set<Long> validateSeenOrderIds(OrderBookSnapshot book, Path path) {
        Set<Long> seen = new HashSet<>(book.seenOrderIds().size() * 2);
        for (Long orderId : book.seenOrderIds()) {
            if (orderId == null || orderId <= 0) {
                throw fail(path, "book seenOrderIds contains invalid id: " + book.symbol());
            }
            if (!seen.add(orderId)) {
                throw fail(path, "book seenOrderIds contains duplicate: " + book.symbol()
                        + ", orderId=" + orderId);
            }
        }
        return seen;
    }

    private void validateLevels(OrderBookSnapshot book,
                                List<PriceLevelSnapshot> levels,
                                OrderSide side,
                                Set<Long> seen,
                                Path path) {
        for (PriceLevelSnapshot level : levels) {
            if (level == null || level.orders() == null || level.orders().isEmpty()) {
                throw fail(path, "book has empty price level: " + book.symbol());
            }
            if (level.price() <= 0) {
                throw fail(path, "book price level must be positive: " + book.symbol()
                        + ", price=" + level.price());
            }
            for (BookOrderSnapshot order : level.orders()) {
                validateBookOrder(book, order, side, OrderType.LIMIT, level.price(), seen, path);
            }
        }
    }

    private void validateBookOrder(OrderBookSnapshot book,
                                   BookOrderSnapshot order,
                                   OrderSide side,
                                   OrderType type,
                                   long price,
                                   Set<Long> seen,
                                   Path path) {
        if (order == null) {
            throw fail(path, "book contains null order: " + book.symbol());
        }
        if (order.orderId() <= 0) {
            throw fail(path, "book orderId must be positive: " + book.symbol());
        }
        if (order.quantity() <= 0 || order.remainingQuantity() <= 0
                || order.remainingQuantity() > order.quantity()) {
            throw fail(path, "book order remainingQuantity out of range: " + book.symbol()
                    + ", orderId=" + order.orderId() + ", quantity=" + order.quantity()
                    + ", remaining=" + order.remainingQuantity());
        }
        if (order.side() != side) {
            throw fail(path, "book order side mismatch: " + book.symbol()
                    + ", orderId=" + order.orderId() + ", expected=" + side
                    + ", actual=" + order.side());
        }
        if (order.type() != type) {
            throw fail(path, "book order type mismatch: " + book.symbol()
                    + ", orderId=" + order.orderId() + ", expected=" + type
                    + ", actual=" + order.type());
        }
        if (order.price() != price) {
            throw fail(path, "book order price mismatch: " + book.symbol()
                    + ", orderId=" + order.orderId() + ", expected=" + price
                    + ", actual=" + order.price());
        }
        if (!book.symbol().equals(order.symbol())) {
            throw fail(path, "book order symbol mismatch: book=" + book.symbol()
                    + ", orderId=" + order.orderId() + ", orderSymbol=" + order.symbol());
        }
        if (!seen.contains(order.orderId())) {
            throw fail(path, "resting order is not in seenOrderIds: " + book.symbol()
                    + ", orderId=" + order.orderId());
        }
        parseInstant(order.placedAt(), path, "book.placedAt=" + order.orderId());
    }

    /**
     * 账户 / 委托 / 订单簿之间的业务关系校验。
     *
     * <p>核心规则：
     * <ul>
     *   <li>每笔挂单必须能找到委托快照，且 userId / symbol / side / type / price /
     *       quantity 完全一致；</li>
     *   <li>委托的 {@code filledQuantity + 挂单剩余量 == quantity}，防止成交进度与
     *       订单簿剩余量不一致；</li>
     *   <li>进行中的委托（{@code PAID}，或 {@code finalAt} 为空的 {@code PARTIALLY_FILLED}）
     *       必须仍在订单簿中；</li>
     *   <li>已进入终态的委托（{@code FILLED} / {@code CANCELED} / {@code finalAt} 非空的
     *       {@code PARTIALLY_FILLED}）必须出现在同 symbol 订单簿的 seenOrderIds 中，
     *       证明它确实进入过撮合引擎。</li>
     * </ul>
     */
    private void validateCrossReferences(SnapshotPayload payload,
                                         Map<Long, OrderSnapshot> orderById,
                                         Map<String, OrderBookSnapshot> bookBySymbol,
                                         Path path) {
        Set<Long> restingIds = new HashSet<>();
        Map<String, Set<Long>> seenBySymbol = new HashMap<>(bookBySymbol.size() * 2);
        for (OrderBookSnapshot book : payload.books()) {
            seenBySymbol.put(book.symbol(), new HashSet<>(book.seenOrderIds()));
            for (PriceLevelSnapshot level : book.bids()) {
                for (BookOrderSnapshot bookOrder : level.orders()) {
                    validateCrossReference(bookOrder, orderById, restingIds, path);
                }
            }
            for (PriceLevelSnapshot level : book.asks()) {
                for (BookOrderSnapshot bookOrder : level.orders()) {
                    validateCrossReference(bookOrder, orderById, restingIds, path);
                }
            }
        }

        for (OrderSnapshot order : payload.orderState().orders()) {
            // 终态判定：FILLED / CANCELED，或 finalAt 非空的 PARTIALLY_FILLED（部分成交收尾）。
            boolean open = (order.status() == OrderStatus.PAID
                    || order.status() == OrderStatus.PARTIALLY_FILLED)
                    && order.finalAt() == null;
            if (open && order.type() == OrderType.MARKET) {
                // 市价单按 IOC 处理：撮合当次即进入终态，静止快照中不允许存在未完成的市价单。
                throw fail(path, "market order must not be open in a quiescent snapshot "
                        + "(IOC): orderId=" + order.orderId() + ", status=" + order.status());
            }
            if (open && !restingIds.contains(order.orderId())) {
                throw fail(path, "open order is not resting in a book: orderId="
                        + order.orderId() + ", status=" + order.status());
            }
            boolean reachedEngine = open
                    || order.status() == OrderStatus.FILLED
                    || order.status() == OrderStatus.CANCELED
                    || order.status() == OrderStatus.PARTIALLY_FILLED;
            if (reachedEngine) {
                OrderBookSnapshot book = bookBySymbol.get(order.symbol());
                Set<Long> seen = seenBySymbol.get(order.symbol());
                if (book == null || seen == null || !seen.contains(order.orderId())) {
                    throw fail(path, "order missing from book seenOrderIds: orderId="
                            + order.orderId() + ", symbol=" + order.symbol()
                            + ", status=" + order.status());
                }
            }
        }
    }

    private void validateCrossReference(BookOrderSnapshot bookOrder,
                                        Map<Long, OrderSnapshot> orderById,
                                        Set<Long> restingIds,
                                        Path path) {
        if (!restingIds.add(bookOrder.orderId())) {
            throw fail(path, "order appears in more than one book position: "
                    + bookOrder.orderId());
        }
        OrderSnapshot order = orderById.get(bookOrder.orderId());
        if (order == null) {
            throw fail(path, "resting order has no order snapshot: " + bookOrder.orderId());
        }
        if (order.finalAt() != null) {
            throw fail(path, "terminal order must not rest in a book: orderId="
                    + bookOrder.orderId() + ", status=" + order.status());
        }
        if (order.userId() != bookOrder.userId()
                || !order.symbol().equals(bookOrder.symbol())
                || order.side() != bookOrder.side()
                || order.type() != bookOrder.type()
                || order.price() != bookOrder.price()
                || order.quantity() != bookOrder.quantity()) {
            throw fail(path, "resting order does not match order snapshot: "
                    + bookOrder.orderId());
        }
        if (order.filledQuantity() + bookOrder.remainingQuantity() != order.quantity()) {
            throw fail(path, "resting order fill/remaining mismatch: orderId="
                    + bookOrder.orderId() + ", filled=" + order.filledQuantity()
                    + ", remaining=" + bookOrder.remainingQuantity()
                    + ", quantity=" + order.quantity());
        }
    }

    private void validateInputOffsets(List<TopicPartitionOffset> offsets, Path path) {
        Set<String> keys = new HashSet<>();
        for (TopicPartitionOffset offset : offsets) {
            if (offset == null || offset.topic() == null || offset.topic().isBlank()) {
                throw fail(path, "input offset topic is missing");
            }
            if (offset.group() == null || offset.group().isBlank()) {
                throw fail(path, "input offset group is missing: " + offset.topic());
            }
            if (offset.partition() < 0 || offset.nextOffset() < 0) {
                throw fail(path, "input offset is negative: " + offset.topic()
                        + "/" + offset.partition());
            }
            String key = offset.topic() + "|" + offset.group() + "|" + offset.partition();
            if (!keys.add(key)) {
                throw fail(path, "duplicate input offset: " + key);
            }
        }
    }

    private void validateReplayCheckpoints(List<ReplayCheckpoint> checkpoints, Path path) {
        Set<String> keys = new HashSet<>();
        for (ReplayCheckpoint checkpoint : checkpoints) {
            if (checkpoint == null || checkpoint.topic() == null
                    || checkpoint.topic().isBlank()) {
                throw fail(path, "replay checkpoint topic is missing");
            }
            if (checkpoint.partition() < 0 || checkpoint.nextOffset() < 0) {
                throw fail(path, "replay checkpoint is negative: " + checkpoint.topic()
                        + "/" + checkpoint.partition());
            }
            String key = checkpoint.topic() + "|" + checkpoint.partition();
            if (!keys.add(key)) {
                throw fail(path, "duplicate replay checkpoint: " + key);
            }
        }
    }

    private static Instant parseInstant(String value, Path path, String field) {
        if (value == null || value.isBlank()) {
            throw fail(path, field + " is missing");
        }
        try {
            return Instant.parse(value);
        } catch (RuntimeException e) {
            throw fail(path, field + " is not an ISO-8601 instant: " + value);
        }
    }

    private static SnapshotRecoveryException fail(Path path, String message) {
        return new SnapshotRecoveryException("invalid snapshot " + path + ": " + message);
    }

    /** 加载结果：规范化后的路径与已校验的文件内容。 */
    public record LoadedSnapshot(Path path, SnapshotFile file) {

        public SnapshotPayload payload() {
            return file.payload();
        }
    }
}
