package com.me.order.service.impl;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettlementCommand;
import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelBeginResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderLookupRecord;
import com.me.order.dto.OrderSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.service.OrderService;
import com.me.order.vo.OrderView;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.event.TradeEvent;
import com.me.shared.money.Amounts;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderRequestLimits;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 内存委托实现：保存委托 → 发出冻结命令 → 处理冻结回执 → 已支付 / 已拒绝。
 *
 * <p>下单命令只由 {@code OrderCommandListener} 从 Kafka {@code me.order.commands-batch} 消费后
 * 调用 {@link #apply(com.me.shared.order.OrderCommand)}，Kafka 是唯一的下单入口。
 *
 * <p>委托按 {@code orderId} 保存在 {@link ConcurrentHashMap} 中，不落库；进程重启后委托全部丢失。
 *
 * <p><b>重复命令语义</b>：{@code me.order.commands-batch} 只由应用内部的
 * {@code OrderCommandBatchPublisher} 写入（幂等 producer，不重发），消费端把输出与
 * 输入 offset 放在同一个 Kafka 事务提交，失败即 fatal，因此生产链路不会把同一条命令
 * 处理两次。同一 orderId + requestId 再次出现说明合同被破坏（时钟回拨撞号、代码回归
 * 或新增外部/重试入口）：这里显式拒绝并记录 error，<b>不再重发冻结命令</b>；同一
 * orderId 换 requestId 返回 {@code ORDER_ID_ALREADY_EXISTS}；成交重投由订单状态
 * （已 FILLED / PARTIALLY_FILLED 不可再成交）拒绝。
 *
 * <p>{@code userId + requestId → 最新 orderId} 的索引只服务于按 requestId 查询，
 * 不参与防重：同一 requestId 重复下单仍会生成新委托，索引返回其中最新的一笔。
 *
 * <p><b>冻结金额</b>：限价买单冻结计价币 {@code price × quantity}；市价买入冻结前端提交的
 * 计价币预算 {@code amount}（参考价 × 1.05 × 数量），撮合完成后未用完的部分退回；卖单冻结
 * 基础币 {@code quantity}。
 *
 * <p><b>状态机</b>：{@code ACCEPTED}（已保存，等待冻结）
 * → {@code PAID}（冻结成功，交给进程内撮合）或 {@code REJECTED}（冻结失败）
 * → {@code PARTIALLY_FILLED} / {@code FILLED}（成交清算）。
 * {@code PARTIALLY_FILLED} 在 {@code finalAt == null} 时表示仍在进行中（委托还挂在
 * 订单簿中），{@code finalAt != null} 时表示终态：剩余部分已撤销并退款（市价单 IOC
 * 收尾、限价单部分成交后撤单）。没有任何成交的撤单/市价单收尾进入 {@code CANCELED}。
 * 状态每推进一次 {@code seq} 加 1。
 *
 * <p><b>成交清算</b>：{@code OrderMatchingService} 在冻结回执监听线程中同步调用
 * {@link #onTrade(TradeEvent)}；taker / maker 两笔委托同时推进成交进度，并生成一条包含买方、
 * 卖方全部扣账 / 到账资金腿的 {@link TradeSettlementCommand}，交给账户模块一次性结清。
 */
@Service
public class OrderServiceImpl implements OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceImpl.class);

    /** 单次当前委托查询最多返回的记录数。 */
    private static final int MAX_OPEN_ORDERS = 200;

    /** 冻结命令 requestId 前缀，后接 orderId；账户模块不做 requestId 去重，前缀仅用于定位委托。 */
    private static final String FREEZE_REQUEST_PREFIX = "order-freeze:";

    private final ConcurrentHashMap<Long, Order> orders = new ConcurrentHashMap<>();

    /**
     * 按 requestId 查询的辅助索引：{@code (userId, requestId) → 最新的 orderId}。
     *
     * <p>同一 requestId 重复下单会生成多个 orderId，这里用 {@code Math.max} 保留最新一笔，
     * 与历史查询 {@code ORDER BY final_at DESC, order_id DESC LIMIT 1} 的口径一致。
     * 索引是派生数据，不进入快照；{@link #restoreState} 时按恢复后的委托重建。
     */
    private final ConcurrentHashMap<RequestKey, Long> orderIdsByRequest = new ConcurrentHashMap<>();

    /**
     * 成交结算 requestId 生成器：演示版用当前毫秒时间戳起步的进程内计数器，避免每笔成交
     * 做「前缀 + 两个 long + 冒号」的字符串拼接。后续会随内存状态一起持久化/恢复；
     * 当前进程重启会重置，跨实例也不保证唯一。
     */
    private final AtomicLong tradeSettlementIdGenerator =
            new AtomicLong(System.currentTimeMillis());

    @Override
    public OrderApplyResult apply(OrderCommand command) {
        return applyInternal(command);
    }

    private OrderApplyResult applyInternal(OrderCommand command) {
        String invalidReason = validate(command);
        if (invalidReason != null) {
            return OrderApplyResult.rejected(invalidReason);
        }

        ParsedSymbol pair = ParsedSymbol.parse(command.symbol());

        long freezeAmount;
        try {
            if (command.side() == OrderSide.SELL) {
                freezeAmount = command.quantity();
            } else if (command.type() == OrderType.MARKET) {
                // 市价买入冻结前端算好的计价币预算；撮合按实际卖价消耗，余量退回。
                freezeAmount = command.amount();
            } else {
                freezeAmount = Amounts.quoteAmount(command.price(), command.quantity(),
                        command.priceScale(), command.quantityScale(), command.quoteScale());
            }
        } catch (ArithmeticException | IllegalArgumentException e) {
            return OrderApplyResult.rejected("AMOUNT_OVERFLOW");
        }
        String freezeCurrency = command.side() == OrderSide.BUY ? pair.quote() : pair.base();

        Order order = new Order(
                command.requestId(),
                command.orderId(),
                command.userId(),
                command.symbol(),
                command.side(),
                command.type(),
                command.price(),
                command.quantity(),
                freezeCurrency,
                freezeAmount,
                0L,
                0L,
                OrderStatus.ACCEPTED,
                1L,
                Instant.now(),
                null,
                command.priceScale(),
                command.quantityScale(),
                command.quoteScale(),
                false);

        Order existing = orders.putIfAbsent(order.orderId(), order);
        if (existing != null) {
            if (existing.requestId().equals(command.requestId())) {
                // 生产链路不会重投同一命令（应用内 producer 不重发，消费端输出与 offset
                // 同事务、失败 fatal）。重复只可能来自合同被破坏，例如时钟回拨撞号、
                // 代码回归或新增外部/重试入口；重发冻结会永久多冻一笔，这里显式拒绝。
                log.error("order rejected, duplicate orderId + requestId: orderId={}, "
                        + "requestId={}", command.orderId(), command.requestId());
                return OrderApplyResult.rejected("ORDER_DUPLICATE_COMMAND");
            }
            log.warn("order rejected, orderId already exists: orderId={}, requestId={}",
                    command.orderId(), command.requestId());
            return OrderApplyResult.rejected("ORDER_ID_ALREADY_EXISTS");
        }
        indexByRequest(order);

        return successOf(order);
    }

    @Override
    public OrderLookupRecord findByRequestId(long userId, String requestId) {
        if (userId <= 0) {
            return null;
        }
        String normalizedRequestId = normalizeRequestId(requestId);
        if (normalizedRequestId == null) {
            return null;
        }
        Long orderId = orderIdsByRequest.get(new RequestKey(userId, normalizedRequestId));
        if (orderId == null) {
            return null;
        }
        Order order = orders.get(orderId);
        if (order == null || order.userId() != userId
                || !normalizedRequestId.equals(normalizeRequestId(order.requestId()))) {
            return null;
        }
        return order.toLookupRecord();
    }

    /** 把委托写入 requestId 查询索引；同一 requestId 保留最新（orderId 最大）的一笔。 */
    private void indexByRequest(Order order) {
        String normalizedRequestId = normalizeRequestId(order.requestId());
        if (normalizedRequestId == null) {
            return;
        }
        orderIdsByRequest.merge(
                new RequestKey(order.userId(), normalizedRequestId),
                order.orderId(),
                Math::max);
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null) {
            return null;
        }
        String normalized = requestId.strip();
        return normalized.isEmpty() ? null : normalized;
    }

    @Override
    public OrderFreezeApplyResult onFreezeResult(OrderFreezeResultEvent event) {
        if (event == null || event.orderId() <= 0) {
            return OrderFreezeApplyResult.ignored("ORDER_ID_INVALID");
        }

        AtomicReference<OrderFreezeApplyResult> resultRef = new AtomicReference<>();
        orders.computeIfPresent(event.orderId(), (orderId, order) -> {
            if (!freezeRequestId(orderId).equals(event.requestId())) {
                resultRef.set(OrderFreezeApplyResult.ignored("FREEZE_REQUEST_MISMATCH"));
                return order;
            }

            if (!event.success()) {
                String reason = event.reason() == null ? "FREEZE_REJECTED" : event.reason();
                if (order.status() == OrderStatus.ACCEPTED) {
                    Order rejected = order.withStatus(OrderStatus.REJECTED);
                    log.warn("order rejected by freeze failure: orderId={}, requestId={}, reason={}",
                            orderId, event.requestId(), reason);
                    resultRef.set(OrderFreezeApplyResult.rejected(reason));
                    return rejected;
                }
                resultRef.set(OrderFreezeApplyResult.rejected(reason));
                return order;
            }

            if (order.status() == OrderStatus.ACCEPTED) {
                Order paid = order.withStatus(OrderStatus.PAID);
                resultRef.set(OrderFreezeApplyResult.placed(paid.toPlacedEvent()));
                return paid;
            }

            if (order.status() == OrderStatus.PAID
                    || order.status() == OrderStatus.PARTIALLY_FILLED
                    || order.status() == OrderStatus.FILLED) {
                // 回执可能因事务重试/重复命令被再次投递。已支付（甚至已成交）时仍返回 placedEvent，
                // 撮合引擎按 orderId 返回 duplicate，本次不再产生撮合对/结算（演示版不重放输出）。
                resultRef.set(OrderFreezeApplyResult.placed(order.toPlacedEvent()));
                return order;
            }

            resultRef.set(OrderFreezeApplyResult.ignored("ORDER_STATUS_" + order.status()));
            return order;
        });

        OrderFreezeApplyResult result = resultRef.get();
        if (result == null) {
            log.warn("freeze result for unknown order ignored: orderId={}, requestId={}, success={}",
                    event.orderId(), event.requestId(), event.success());
            return OrderFreezeApplyResult.ignored("ORDER_NOT_FOUND");
        }
        return result;
    }

    @Override
    public OrderTradeApplyResult onTrade(TradeEvent trade) {
        // 成交由进程内撮合产生，双方委托必然存在、属于该交易对且处于可成交状态；
        // 撮合、清算与撤单共用 orderLock，这里不为不可达的状态漂移保留失败分支。
        Order takerOrder = fillOrder(trade.takerOrderId(), trade);
        Order makerOrder = fillOrder(trade.makerOrderId(), trade);
        List<OrderHistoryEvent> historyEvents = new ArrayList<>(2);
        if (takerOrder.status() == OrderStatus.FILLED) {
            historyEvents.add(takerOrder.toHistoryEvent());
        }
        if (makerOrder.status() == OrderStatus.FILLED) {
            historyEvents.add(makerOrder.toHistoryEvent());
        }
        boolean takerIsBuyer = trade.takerSide() == OrderSide.BUY;
        long buyerUserId = takerIsBuyer ? takerOrder.userId() : makerOrder.userId();
        long sellerUserId = takerIsBuyer ? makerOrder.userId() : takerOrder.userId();
        Order buyerOrder = takerIsBuyer ? takerOrder : makerOrder;
        ParsedSymbol pair = ParsedSymbol.parse(trade.symbol());

        // 清算拆成买方、卖方两条单用户命令，账户侧各自按 userId slot 取锁，
        // 不再依赖跨用户原子性（所有金额已在下单冻结阶段扣除）。
        List<TradeSettlementCommand.SettlementLeg> buyerLegs = new ArrayList<>(3);
        // 买方：冻结计价币扣账（支付），可用基础币入账（到账）。
        buyerLegs.add(new TradeSettlementCommand.SettlementLeg(
                buyerUserId, pair.quote(), FundsLogType.TRADE_SETTLE_OUT, trade.amount()));
        buyerLegs.add(new TradeSettlementCommand.SettlementLeg(
                buyerUserId, pair.base(), FundsLogType.TRADE_SETTLE_IN, trade.quantity()));
        List<TradeSettlementCommand.SettlementLeg> sellerLegs = new ArrayList<>(2);
        // 卖方：冻结基础币扣账（交付），可用计价币入账（到账）。
        sellerLegs.add(new TradeSettlementCommand.SettlementLeg(
                sellerUserId, pair.base(), FundsLogType.TRADE_SETTLE_OUT, trade.quantity()));
        sellerLegs.add(new TradeSettlementCommand.SettlementLeg(
                sellerUserId, pair.quote(), FundsLogType.TRADE_SETTLE_IN, trade.amount()));

        // 买单全部成交且成交价优于限价时，把多冻结的计价币释放回可用。
        long refund = buyerOrder.frozenAmount() - buyerOrder.settledQuoteAmount();
        if (buyerOrder.status() == OrderStatus.FILLED && refund > 0) {
            buyerLegs.add(new TradeSettlementCommand.SettlementLeg(
                    buyerUserId, buyerOrder.frozenCurrency(),
                    FundsLogType.ORDER_UNFREEZE, refund));
        }

        // 一个成交对生成两条单用户命令；每条命令独立取一个 requestId，
        // 唯一性由生成器保证，不再拼 -b/-s 后缀。计数器随内存状态持久化/恢复。
        long buyerSettlementId = tradeSettlementIdGenerator.getAndIncrement();
        long sellerSettlementId = tradeSettlementIdGenerator.getAndIncrement();
        TradeSettlementCommand buyerCommand = new TradeSettlementCommand(
                Long.toString(buyerSettlementId),
                trade.tradeId(),
                trade.symbol(),
                trade.takerOrderId(),
                trade.makerOrderId(),
                buyerLegs);
        TradeSettlementCommand sellerCommand = new TradeSettlementCommand(
                Long.toString(sellerSettlementId),
                trade.tradeId(),
                trade.symbol(),
                trade.takerOrderId(),
                trade.makerOrderId(),
                sellerLegs);

        return OrderTradeApplyResult.of(
                List.of(buyerCommand, sellerCommand), historyEvents);
    }

    @Override
    public OrderTradeApplyResult completeMarketOrder(long orderId) {
        AtomicReference<OrderTradeApplyResult> resultRef = new AtomicReference<>();
        orders.compute(orderId, (id, order) -> {
            if (order == null || order.type() != OrderType.MARKET) {
                resultRef.set(OrderTradeApplyResult.empty());
                return order;
            }
            if (order.status() == OrderStatus.FILLED
                    || order.status() == OrderStatus.CANCELED
                    || order.finalAt() != null) {
                // 最后一笔成交已把委托推进为 FILLED（买方结算命令已携带退款腿），
                // 或委托已经收尾进入终态（含部分成交终态），这里不能再产生退款。
                resultRef.set(OrderTradeApplyResult.empty());
                return order;
            }

            // 市价单按 IOC 处理：撮合结束后未成交部分撤销，不挂单。
            // 买单退回未用完的计价币预算，卖单退回未成交的基础币。
            long consumed = order.side() == OrderSide.BUY
                    ? order.settledQuoteAmount()
                    : order.filledQuantity();
            long refund = order.frozenAmount() - consumed;
            if (refund < 0) {
                log.warn("market order completion refund negative, clamp to zero: orderId={}, "
                        + "frozen={}, consumed={}", orderId, order.frozenAmount(), consumed);
                refund = 0L;
            }

            // 有成交量时保持 PARTIALLY_FILLED 并固定 finalAt 进入终态；完全没有成交才进入 CANCELED。
            Order completed = order.filledQuantity() > 0
                    ? order.withTerminalStatus(OrderStatus.PARTIALLY_FILLED)
                    : order.withStatus(OrderStatus.CANCELED);
            OrderHistoryEvent historyEvent = completed.toHistoryEvent();
            if (refund == 0) {
                resultRef.set(OrderTradeApplyResult.of(List.of(), List.of(historyEvent)));
                return completed;
            }

            long settlementId = tradeSettlementIdGenerator.getAndIncrement();
            TradeSettlementCommand refundCommand = new TradeSettlementCommand(
                    Long.toString(settlementId) + "-r",
                    0L,
                    order.symbol(),
                    order.orderId(),
                    0L,
                    List.of(new TradeSettlementCommand.SettlementLeg(
                            order.userId(), order.frozenCurrency(),
                            FundsLogType.ORDER_UNFREEZE, refund)));
            resultRef.set(OrderTradeApplyResult.of(
                    List.of(refundCommand), List.of(historyEvent)));
            return completed;
        });
        return resultRef.get();
    }

    @Override
    public OrderCancelPrepareResult prepareCancel(long orderId, long userId, String symbol) {
        if (orderId <= 0) {
            return OrderCancelPrepareResult.rejected("ORDER_ID_INVALID");
        }
        if (userId <= 0) {
            return OrderCancelPrepareResult.rejected("USER_ID_INVALID");
        }
        if (symbol == null || symbol.isBlank()) {
            return OrderCancelPrepareResult.rejected("SYMBOL_REQUIRED");
        }
        String normalizedSymbol = symbol.strip().toUpperCase(Locale.ROOT);
        Order order = orders.get(orderId);
        if (order == null) {
            return OrderCancelPrepareResult.rejected("ORDER_NOT_FOUND");
        }
        if (!order.symbol().equals(normalizedSymbol)) {
            return OrderCancelPrepareResult.rejected("SYMBOL_MISMATCH");
        }
        if (order.userId() != userId) {
            return OrderCancelPrepareResult.rejected("USER_MISMATCH");
        }
        return switch (order.status()) {
            case PAID -> OrderCancelPrepareResult.ready(cancelContext(order));
            // 部分成交只有在 finalAt == null（仍在簿中）时可撤；已收尾的部分成交是终态。
            case PARTIALLY_FILLED -> order.finalAt() == null
                    ? OrderCancelPrepareResult.ready(cancelContext(order))
                    : OrderCancelPrepareResult.rejected("ORDER_ALREADY_COMPLETED");
            // 已受理但冻结回执未到：此时撤销会与回执竞争，拒绝并让调用方稍后重试。
            case ACCEPTED -> OrderCancelPrepareResult.rejected("ORDER_NOT_PAID_YET");
            case CANCELED -> OrderCancelPrepareResult.rejected("ORDER_ALREADY_CANCELED");
            case FILLED -> OrderCancelPrepareResult.rejected("ORDER_ALREADY_FILLED");
            case REJECTED -> OrderCancelPrepareResult.rejected("ORDER_REJECTED");
        };
    }

    private static OrderCancelContext cancelContext(Order order) {
        return new OrderCancelContext(
                order.orderId(), order.userId(), order.symbol(), order.side(), order.type(),
                order.price(), order.status(), order.quantity(), order.filledQuantity(),
                order.frozenCurrency(), order.frozenAmount(), order.settledQuoteAmount());
    }

    @Override
    public OrderCancelBeginResult beginCancel(OrderCancelContext context) {
        if (context == null || context.orderId() <= 0) {
            return OrderCancelBeginResult.rejected("CANCEL_CONTEXT_INVALID");
        }
        AtomicReference<OrderCancelBeginResult> resultRef = new AtomicReference<>();
        orders.computeIfPresent(context.orderId(), (orderId, order) -> {
            if (!order.symbol().equals(context.symbol())) {
                resultRef.set(OrderCancelBeginResult.rejected("SYMBOL_MISMATCH"));
                return order;
            }
            if (order.cancelRequested()) {
                // 同一撤单命令重试 / 批内重复：标记已存在，继续交给更新阶段（幂等）。
                resultRef.set(OrderCancelBeginResult.allowed());
                return order;
            }
            if (!isCancelable(order)) {
                resultRef.set(OrderCancelBeginResult.rejected(
                        "ORDER_STATUS_" + order.status()));
                return order;
            }
            resultRef.set(OrderCancelBeginResult.allowed());
            return order.withCancelRequested();
        });
        OrderCancelBeginResult result = resultRef.get();
        return result == null
                ? OrderCancelBeginResult.rejected("ORDER_NOT_FOUND")
                : result;
    }

    @Override
    public boolean isCancelPending(long orderId) {
        Order order = orders.get(orderId);
        return order != null && order.cancelRequested();
    }

    @Override
    public OrderCancelApplyResult applyCancel(OrderCancelContext context) {
        if (context == null || context.orderId() <= 0) {
            return OrderCancelApplyResult.ignored("CANCEL_CONTEXT_INVALID");
        }
        AtomicReference<OrderCancelApplyResult> resultRef = new AtomicReference<>();
        orders.computeIfPresent(context.orderId(), (orderId, order) -> {
            if (!order.symbol().equals(context.symbol())) {
                resultRef.set(OrderCancelApplyResult.ignored("SYMBOL_MISMATCH"));
                return order;
            }
            if (!isCancelable(order)) {
                // 撤单准备后可能被成交清算、IOC 收尾或另一条撤单命令抢先推进；不重复解冻。
                resultRef.set(OrderCancelApplyResult.ignored("ORDER_STATUS_" + order.status()));
                return order;
            }

            // 买单冻结计价币，已成交部分按 settledQuoteAmount 扣减；卖单冻结基础币，
            // 已成交部分按 filledQuantity 扣减。剩余冻结在撤单时全部退回可用。
            long consumed = order.side() == OrderSide.BUY
                    ? order.settledQuoteAmount()
                    : order.filledQuantity();
            long refund = order.frozenAmount() - consumed;
            if (refund < 0) {
                log.warn("cancel refund negative, clamp to zero: orderId={}, frozen={}, consumed={}",
                        orderId, order.frozenAmount(), consumed);
                refund = 0L;
            }

            // 有成交量的委托以 PARTIALLY_FILLED 终态结束，完全没有成交才进入 CANCELED。
            Order completed = order.filledQuantity() > 0
                    ? order.withTerminalStatus(OrderStatus.PARTIALLY_FILLED)
                    : order.withStatus(OrderStatus.CANCELED);
            OrderHistoryEvent historyEvent = completed.toHistoryEvent();
            if (refund == 0) {
                resultRef.set(OrderCancelApplyResult.canceled(
                        0L, List.of(), historyEvent));
                return completed;
            }

            long settlementId = tradeSettlementIdGenerator.getAndIncrement();
            TradeSettlementCommand command = new TradeSettlementCommand(
                    Long.toString(settlementId) + "-c",
                    0L,
                    order.symbol(),
                    order.orderId(),
                    0L,
                    List.of(new TradeSettlementCommand.SettlementLeg(
                            order.userId(), order.frozenCurrency(),
                            FundsLogType.ORDER_UNFREEZE, refund)));
            resultRef.set(OrderCancelApplyResult.canceled(
                    refund, List.of(command), historyEvent));
            return completed;
        });
        OrderCancelApplyResult result = resultRef.get();
        return result == null ? OrderCancelApplyResult.ignored("ORDER_NOT_FOUND") : result;
    }

    private Order fillOrder(long orderId, TradeEvent trade) {
        return orders.compute(orderId, (id, order) ->
                order.withFill(trade.quantity(), trade.amount()));
    }

    @Override
    public OrderView view(long orderId) {
        Order order = orders.get(orderId);
        return order == null ? null : order.toView();
    }

    @Override
    public List<OrderView> openOrders(long userId, String symbol, int limit) {
        if (userId <= 0) {
            throw new IllegalArgumentException("userId must be positive: " + userId);
        }
        String normalizedSymbol = symbol == null || symbol.isBlank()
                ? null
                : symbol.strip().toUpperCase(Locale.ROOT);
        int safeLimit = Math.max(1, Math.min(limit, MAX_OPEN_ORDERS));
        return orders.values().stream()
                .filter(order -> order.userId() == userId)
                .filter(order -> normalizedSymbol == null
                        || order.symbol().equals(normalizedSymbol))
                .filter(OrderServiceImpl::isOpen)
                .sorted(Comparator.comparing(Order::createdAt)
                        .thenComparingLong(Order::orderId)
                        .reversed())
                .limit(safeLimit)
                .map(Order::toView)
                .toList();
    }

    /**
     * 委托是否仍在进行中：{@code PARTIALLY_FILLED} 只有在 {@code finalAt == null}
     * （仍在订单簿中等待成交）时才算未完成；已收尾的部分成交是终态。
     */
    private static boolean isOpen(Order order) {
        return order.status() == OrderStatus.ACCEPTED
                || order.status() == OrderStatus.PAID
                || (order.status() == OrderStatus.PARTIALLY_FILLED && order.finalAt() == null);
    }

    /** 可撤状态：PAID，或仍在进行中的部分成交（{@code finalAt == null}）。 */
    private static boolean isCancelable(Order order) {
        return order.status() == OrderStatus.PAID
                || (order.status() == OrderStatus.PARTIALLY_FILLED && order.finalAt() == null);
    }

    @Override
    public OrderStateSnapshot snapshotState() {
        List<OrderSnapshot> snapshots = new ArrayList<>(orders.size());
        for (Order order : orders.values()) {
            snapshots.add(order.toSnapshot());
        }
        snapshots.sort(Comparator.comparingLong(OrderSnapshot::orderId));
        return new OrderStateSnapshot(List.copyOf(snapshots),
                tradeSettlementIdGenerator.get());
    }

    @Override
    public void restoreState(OrderStateSnapshot state) {
        if (state == null || state.orders() == null) {
            throw new IllegalArgumentException("snapshot order state must not be null");
        }
        if (state.tradeSettlementIdGenerator() < 1) {
            throw new IllegalArgumentException("snapshot tradeSettlementIdGenerator must be "
                    + "positive: " + state.tradeSettlementIdGenerator());
        }
        Map<Long, Order> restored = new HashMap<>(state.orders().size() * 2);
        for (OrderSnapshot snapshot : state.orders()) {
            Order order = Order.fromSnapshot(snapshot);
            if (restored.putIfAbsent(order.orderId(), order) != null) {
                throw new IllegalArgumentException(
                        "duplicate snapshot orderId: " + order.orderId());
            }
        }
        orders.clear();
        orders.putAll(restored);
        orderIdsByRequest.clear();
        for (Order order : restored.values()) {
            indexByRequest(order);
        }
        tradeSettlementIdGenerator.set(state.tradeSettlementIdGenerator());
    }

    private OrderApplyResult successOf(Order order) {
        FundsCommand freezeCommand = order.status() == OrderStatus.ACCEPTED ? freezeCommand(order) : null;
        return OrderApplyResult.success(order.toView(), freezeCommand);
    }

    private FundsCommand freezeCommand(Order order) {
        return new FundsCommand(
                freezeRequestId(order.orderId()),
                order.userId(),
                order.frozenCurrency(),
                FundsLogType.ORDER_FREEZE,
                order.frozenAmount(),
                order.orderId(),
                order.symbol());
    }

    private static String freezeRequestId(long orderId) {
        return FREEZE_REQUEST_PREFIX + orderId;
    }

    private String validate(OrderCommand command) {
        if (command == null) {
            return "COMMAND_IS_NULL";
        }
        if (command.requestId() == null || command.requestId().isBlank()) {
            return "REQUEST_ID_REQUIRED";
        }
        if (command.requestId().strip().length() > OrderRequestLimits.MAX_REQUEST_ID_LENGTH) {
            return "REQUEST_ID_INVALID";
        }
        if (command.orderId() <= 0) {
            return "ORDER_ID_INVALID";
        }
        if (command.userId() <= 0) {
            return "USER_ID_INVALID";
        }
        if (command.symbol() == null || command.symbol().isBlank()) {
            return "SYMBOL_REQUIRED";
        }
        if (ParsedSymbol.parse(command.symbol()) == null) {
            return "SYMBOL_INVALID";
        }
        if (command.side() == null) {
            return "SIDE_REQUIRED";
        }
        if (command.type() == null) {
            return "TYPE_REQUIRED";
        }
        if (command.price() < 0) {
            return "PRICE_INVALID";
        }
        if (command.type() == OrderType.LIMIT && command.price() == 0) {
            return "PRICE_REQUIRED_FOR_LIMIT";
        }
        if (command.type() == OrderType.MARKET && command.price() != 0) {
            return "PRICE_NOT_ALLOWED_FOR_MARKET";
        }
        if (command.quantity() <= 0) {
            return "QUANTITY_INVALID";
        }
        if (command.side() == OrderSide.BUY && command.type() == OrderType.MARKET) {
            if (command.amount() <= 0) {
                return "AMOUNT_REQUIRED_FOR_MARKET_BUY";
            }
        } else if (command.amount() != 0) {
            return "AMOUNT_NOT_ALLOWED";
        }
        return null;
    }

    /** requestId 查询索引的键：用户内 requestId 唯一。 */
    private record RequestKey(long userId, String requestId) {
    }

    private record Order(
            String requestId,
            long orderId,
            long userId,
            String symbol,
            OrderSide side,
            OrderType type,
            long price,
            long quantity,
            String frozenCurrency,
            long frozenAmount,
            long filledQuantity,
            long settledQuoteAmount,
            OrderStatus status,
            long seq,
            Instant createdAt,
            Instant finalAt,
            int priceScale,
            int quantityScale,
            int quoteScale,
            boolean cancelRequested) {

        private Order withStatus(OrderStatus newStatus) {
            return new Order(requestId, orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, filledQuantity, settledQuoteAmount,
                    newStatus, seq + 1, createdAt, terminalTime(newStatus, finalAt),
                    priceScale, quantityScale, quoteScale, cancelRequested);
        }

        /**
         * 推进为终态并固定 {@code finalAt}：用于有成交量的委托收尾（市价单 IOC 剩余撤销、
         * 部分成交后撤单）。状态保持 {@code PARTIALLY_FILLED}，以 {@code finalAt}
         * 与仍在进行中的部分成交区分。
         */
        private Order withTerminalStatus(OrderStatus newStatus) {
            Instant finalTime = finalAt != null ? finalAt : Instant.now();
            return new Order(requestId, orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, filledQuantity, settledQuoteAmount,
                    newStatus, seq + 1, createdAt, finalTime,
                    priceScale, quantityScale, quoteScale, cancelRequested);
        }

        /**
         * 标记撤单开始：在引擎对象锁内原子设置，阻止后续撮合把委托重新挂回订单簿。
         * 该标记不改变对外可见状态与 seq，真正的 {@code CANCELED} 推进由
         * {@code applyCancel} 完成。
         */
        private Order withCancelRequested() {
            return new Order(requestId, orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, filledQuantity, settledQuoteAmount,
                    status, seq, createdAt, finalAt, priceScale, quantityScale, quoteScale, true);
        }

        /**
         * 应用一笔成交：累加成交量；买单同时累计成交金额，用于全部成交后计算剩余冻结。
         */
        private Order withFill(long tradeQuantity, long tradeAmount) {
            long filled = Math.addExact(filledQuantity, tradeQuantity);
            if (filled > quantity) {
                throw new IllegalStateException("trade overfills order: orderId=" + orderId
                        + ", filled=" + filled + ", quantity=" + quantity);
            }
            OrderStatus nextStatus = filled == quantity
                    ? OrderStatus.FILLED
                    : OrderStatus.PARTIALLY_FILLED;
            long nextSettledQuote = side == OrderSide.BUY
                    ? Math.addExact(settledQuoteAmount, tradeAmount)
                    : 0L;
            return new Order(requestId, orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, filled, nextSettledQuote,
                    nextStatus, seq + 1, createdAt, terminalTime(nextStatus, finalAt),
                    priceScale, quantityScale, quoteScale, cancelRequested);
        }

        private OrderView toView() {
            return new OrderView(
                    orderId, userId, symbol, side, type, price, quantity, filledQuantity,
                    frozenCurrency, frozenAmount, status, seq, createdAt);
        }

        private OrderLookupRecord toLookupRecord() {
            return new OrderLookupRecord(
                    orderId, requestId, userId, symbol, side, type, status, price, quantity,
                    filledQuantity, frozenCurrency, frozenAmount, priceScale, quantityScale,
                    quoteScale, seq, createdAt, finalAt);
        }

        /**
         * 进入撤销 / 全部成交终态时固定 {@code finalAt}；进行中的部分成交、已支付等
         * 中间状态保持 {@code null}。部分成交后的终态由
         * {@link #withTerminalStatus(OrderStatus)} 固定 {@code finalAt}。
         * 固定时间戳让同一委托重复投递的历史事件在 QuestDB 中可去重。
         */
        private static Instant terminalTime(OrderStatus status, Instant current) {
            if (current != null) {
                return current;
            }
            return status == OrderStatus.CANCELED || status == OrderStatus.FILLED
                    ? Instant.now()
                    : null;
        }

        /** 终态委托的历史快照；由调用方保证 {@code finalAt} 非空。 */
        private OrderHistoryEvent toHistoryEvent() {
            return new OrderHistoryEvent(
                    orderId, requestId, userId, symbol, side, type, status, price, quantity,
                    filledQuantity, frozenCurrency, frozenAmount, priceScale, quantityScale,
                    quoteScale, seq, createdAt, finalAt);
        }

        private OrderPlacedEvent toPlacedEvent() {
            return new OrderPlacedEvent(
                    orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, Instant.now(),
                    priceScale, quantityScale, quoteScale);
        }

        private OrderSnapshot toSnapshot() {
            return new OrderSnapshot(
                    requestId, orderId, userId, symbol, side, type, price, quantity,
                    frozenCurrency, frozenAmount, filledQuantity, settledQuoteAmount,
                    status, seq, createdAt.toString(),
                    finalAt == null ? null : finalAt.toString(),
                    priceScale, quantityScale, quoteScale, cancelRequested);
        }

        /** 从快照重建委托；只允许启动恢复阶段使用。 */
        private static Order fromSnapshot(OrderSnapshot snapshot) {
            if (snapshot == null) {
                throw new IllegalArgumentException("snapshot order must not be null");
            }
            if (snapshot.orderId() <= 0 || snapshot.userId() <= 0) {
                throw new IllegalArgumentException("invalid snapshot order id: " + snapshot);
            }
            if (snapshot.symbol() == null || snapshot.symbol().isBlank()
                    || snapshot.requestId() == null || snapshot.requestId().isBlank()
                    || snapshot.side() == null || snapshot.type() == null
                    || snapshot.status() == null) {
                throw new IllegalArgumentException("incomplete snapshot order: " + snapshot);
            }
            if (snapshot.quantity() <= 0
                    || snapshot.filledQuantity() < 0
                    || snapshot.filledQuantity() > snapshot.quantity()) {
                throw new IllegalArgumentException("invalid snapshot order quantity: " + snapshot);
            }
            Instant parsedCreatedAt;
            Instant parsedFinalAt = null;
            try {
                parsedCreatedAt = Instant.parse(snapshot.createdAt());
                if (snapshot.finalAt() != null) {
                    parsedFinalAt = Instant.parse(snapshot.finalAt());
                }
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "invalid snapshot order timestamp: " + snapshot, e);
            }
            return new Order(
                    snapshot.requestId(),
                    snapshot.orderId(),
                    snapshot.userId(),
                    snapshot.symbol(),
                    snapshot.side(),
                    snapshot.type(),
                    snapshot.price(),
                    snapshot.quantity(),
                    snapshot.frozenCurrency(),
                    snapshot.frozenAmount(),
                    snapshot.filledQuantity(),
                    snapshot.settledQuoteAmount(),
                    snapshot.status(),
                    snapshot.seq(),
                    parsedCreatedAt,
                    parsedFinalAt,
                    snapshot.priceScale(),
                    snapshot.quantityScale(),
                    snapshot.quoteScale(),
                    snapshot.cancelRequested());
        }
    }

    /** 交易对 {@code BASE-QUOTE} 的解析结果。 */
    private record ParsedSymbol(String base, String quote) {

        private static ParsedSymbol parse(String symbol) {
            int dash = symbol.indexOf('-');
            if (dash <= 0 || dash != symbol.lastIndexOf('-') || dash == symbol.length() - 1) {
                return null;
            }
            // 调用方传入的 symbol 已由 OrderCommand 归一化（strip + 大写），
            // 这里直接截取，不再重复 strip。
            String base = symbol.substring(0, dash);
            String quote = symbol.substring(dash + 1);
            return base.isEmpty() || quote.isEmpty() ? null : new ParsedSymbol(base, quote);
        }
    }
}
