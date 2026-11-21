package com.me.order.service;

import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelBeginResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderLookupRecord;
import com.me.order.dto.OrderStateSnapshot;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.vo.OrderView;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.event.TradeEvent;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 委托业务接口：保存委托、处理冻结回执、成交清算、撤单、查询委托。跨模块只调用该接口。
 *
 * <p>下单命令只从 Kafka {@code me.order.commands-batch} 进入：{@link #apply(OrderCommand)} 由
 * {@code OrderCommandListener} 调用，是系统唯一的下单入口。撤单命令只从
 * {@code me.order.cancels-batch} 进入，由 {@code OrderCancelListener} 调用
 * {@link #prepareCancel} / {@link #applyCancel}。
 */
public interface OrderService {

    /**
     * 保存一笔委托并给出需要发送的冻结命令。生产链路不会重复投递同一命令；
     * {@code orderId} 由下单入口统一生成且每单唯一，本方法不做 orderId 重复校验。
     */
    OrderApplyResult apply(OrderCommand command);

    /**
     * 应用账户模块的冻结回执：成功时把委托推进为已支付，失败时推进为已拒绝。
     * 回执与委托不匹配时返回 {@code handled=false}，调用方可以忽略。
     * 委托已支付 / 已成交时，重复的冻结成功回执仍返回 {@code placedEvent}；撮合引擎按
     * orderId 去重，不会重复产生撮合对/结算。
     */
    OrderFreezeApplyResult onFreezeResult(OrderFreezeResultEvent event);

    /**
     * 成交清算：把撮合成交对的两笔委托推进为部分成交 / 已成交，并生成一条包含买方、卖方
     * 扣账与到账资金腿的双边结算命令。委托成交至 {@code FILLED} 时同时生成历史事件，
     * 供监听器在同一个 Kafka 事务里发往 {@code me.order.history-batch}。
     * 由委托侧进程内撮合在引擎锁内同步调用（不再消费 {@code me.trades}）；调用时
     * 成交双方的委托必然已存在且处于可成交状态。
     */
    OrderTradeApplyResult onTrade(TradeEvent trade);

    /**
     * 市价单撮合结束（IOC）：撮合引擎只与对手方限价单成交，未成交部分不挂单。
     * 委托未全部成交时收尾，并生成一条
     * {@link com.me.shared.event.FundsLogType#ORDER_UNFREEZE} 清算命令退回剩余冻结：
     * 买单退回未用完的计价币预算（{@code frozenAmount - settledQuoteAmount}），
     * 卖单退回未成交的基础币（{@code frozenAmount - filledQuantity}）。
     * 有成交量时状态保持 {@code PARTIALLY_FILLED} 并固定 {@code finalAt} 进入终态；
     * 完全没有成交时进入 {@code CANCELED}。
     *
     * <p>若委托在成交过程中已按数量全部成交（{@code FILLED}），退款已由最后一笔成交的
     * 买方结算命令携带（成交价优于限价时），本方法返回空结果，不重复退款。
     * 首次进入终态时同时生成历史事件（IOC 剩余量撤销）。
     */
    OrderTradeApplyResult completeMarketOrder(long orderId);

    /**
     * 撤单准备：查询委托并校验归属、交易对与可撤状态，返回撤单快照。
     *
     * <p>只允许撤 {@code PAID} / 进行中的 {@code PARTIALLY_FILLED}（{@code finalAt == null}）；
     * 已收尾的部分成交是终态，返回 {@code ORDER_ALREADY_COMPLETED}。{@code ACCEPTED} 仍在
     * 冻结回执途中，返回 {@code ORDER_NOT_PAID_YET}，由调用方提示稍后重试。
     * 必须携带 {@code userId > 0}，且只有与委托归属一致时才可撤；不一致返回
     * {@code USER_MISMATCH}，命令被丢弃。
     *
     * <p>本方法只读，由撤单监听器的查询线程池并发调用。
     */
    OrderCancelPrepareResult prepareCancel(long orderId, long userId, String symbol);

    /**
     * 撤单开始：在引擎对象锁内把委托原子标记为“撤单中”，阻止并发撮合把尚未挂单的委托
     * 重新挂回订单簿。标记成功后，{@link #applyCancel} 才能安全地推进为 {@code CANCELED}。
     *
     * <p>委托已处于撤单中（重试 / 批内重复）时返回 {@code accepted=true}，保证幂等；
     * 已成交、已撤销、不存在等返回 {@code accepted=false}。
     */
    OrderCancelBeginResult beginCancel(OrderCancelContext context);

    /**
     * 委托是否已被标记为“撤单中”。{@code OrderMatchingServiceImpl.match} 在引擎对象锁内
     * 检查该标记，已开始的撤单不会再被撮合挂单。
     */
    boolean isCancelPending(long orderId);

    /**
     * 撤单更新：把委托从可撤状态原子推进为终态，并按成交进度计算解冻金额，
     * 生成一条只含 {@code ORDER_UNFREEZE} 腿的结算命令。没有成交量的委托进入
     * {@code CANCELED}；有成交量的委托保持 {@code PARTIALLY_FILLED} 并固定
     * {@code finalAt}（部分成交终态）。
     *
     * <p>解冻金额：买单为 {@code frozenAmount - settledQuoteAmount}，卖单为
     * {@code quantity - filledQuantity}；为 0 时不生成清算命令。委托在准备阶段之后
     * 已全部成交或已被撤销时返回 {@code handled=false}，不重复解冻。
     *
     * <p>本方法由撤单监听器的更新线程池并发调用，按 orderId 在
     * {@link java.util.concurrent.ConcurrentHashMap#computeIfPresent} 中原子推进状态。
     * 首次推进为 {@code CANCELED} 时同时生成历史事件，由监听器随撤单消费批次发往
     * {@code me.order.history-batch}。
     */
    OrderCancelApplyResult applyCancel(OrderCancelContext context);

    /**
     * 终态委托的历史事件已经写入本批输出后调用：从内存委托表移除这些委托。
     *
     * <p>只在 {@link #onTrade} / {@link #completeMarketOrder} / {@link #applyCancel}
     * 产生历史事件，且调用方确认事件已随当前 Kafka 事务输出后调用。重复调用幂等。
     *
     * <p>终态委托移除后，按 requestId 的当前委托查询会回退到 QuestDB 历史委托。
     */
    void onHistoryPublished(List<OrderHistoryEvent> historyEvents);

    /**
     * 按委托号查询；委托不存在时返回 {@code null}。
     */
    OrderView view(long orderId);

    /**
     * 按 {@code userId + requestId} 查询当前内存中的委托；不存在时返回 {@code null}。
     *
     * <p>不限制委托状态：内存中仍保留的委托（含 {@code REJECTED}）都能查到；终态委托
     * 在历史事件发布后已从内存移除，查询方应回退到 QuestDB 历史委托。同一用户重复使用
     * 同一个 {@code requestId} 产生多笔委托时，返回 {@code orderId} 最大（最新）的一笔。
     *
     * <p>本方法只做查询；{@code requestId} 索引不改变演示版“不做 requestId 防重”的重投语义。
     *
     * @param userId    用户 ID，必须为正数
     * @param requestId 请求标识，不能为空
     */
    OrderLookupRecord findByRequestId(long userId, String requestId);

    /**
     * 查询用户当前未完成的委托（{@code ACCEPTED} / {@code PAID} / 进行中的
     * {@code PARTIALLY_FILLED}），按创建时间倒序（最新在前）。已进入终态的委托
     * （含部分成交终态）不返回。
     *
     * @param userId 用户 ID，必须为正数
     * @param symbol 可选交易对筛选；{@code null} / 空白表示全部交易对
     * @param limit  1..200，超出范围按边界夹取
     */
    List<OrderView> openOrders(long userId, String symbol, int limit);

    /**
     * 快照用：返回内存中仍保留的委托（未完成 + {@code REJECTED}；终态委托已在发布
     * 历史事件后移除）及结算 requestId 生成器的只读副本，按 orderId 升序。
     *
     * <p>调用方必须持有委托模块的批次锁（{@code OrderBatchLock}），否则只能得到
     * {@link java.util.concurrent.ConcurrentHashMap} 的弱一致遍历结果。快照服务在
     * orderLock 内调用本方法。
     */
    OrderStateSnapshot snapshotState();

    /**
     * 快照恢复用：清空当前委托并恢复结算 requestId 生成器。
     *
     * <p>只能在启动恢复阶段、consumer 尚未启动时调用；实现不做并发保护。
     *
     * @throws IllegalArgumentException 快照包含重复委托或非法字段
     */
    void restoreState(OrderStateSnapshot state);
}
