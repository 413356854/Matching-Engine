package com.me.order.service;

import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelBeginResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.vo.OrderView;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.event.TradeEvent;
import com.me.shared.order.OrderCommand;
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
     * 保存一笔委托并给出需要发送的冻结命令。同一 {@code orderId + requestId} 重投时返回已有
     * 委托的当前状态；同一 {@code orderId} 换 {@code requestId} 返回拒绝。演示版不做 requestId
     * 级别的缓存，后续由持久化状态恢复替代。
     */
    OrderApplyResult apply(OrderCommand command);

    /**
     * 应用账户模块的冻结回执：成功时把委托推进为已支付，失败时推进为已拒绝。
     * 回执与委托不匹配时返回 {@code handled=false}，调用方可以忽略。
     * 委托已支付 / 已成交时，重复的冻结成功回执仍返回 {@code placedEvent}；撮合引擎按
     * orderId 去重，不会重复产生行情/结算。
     */
    OrderFreezeApplyResult onFreezeResult(OrderFreezeResultEvent event);

    /**
     * 成交清算：把撮合成交对的两笔委托推进为部分成交 / 已成交，并生成一条包含买方、卖方
     * 扣账与到账资金腿的双边结算命令。由委托侧进程内撮合编排同步调用（不再消费
     * {@code me.trades}）。演示版不缓存成交对；重复调用时由订单状态（已全部成交）拒绝，
     * 部分成交的重复回执不保证去重。
     */
    OrderTradeApplyResult onTrade(TradeEvent trade);

    /**
     * 市价买入撮合结束：委托标记为已成交（撮合完成），未用完的冻结预算生成一条
     * {@link com.me.shared.event.FundsLogType#ORDER_UNFREEZE} 退款清算命令退回可用余额。
     *
     * <p>市价买入是「金额驱动」的即时单：撮合引擎按剩余预算限制成交量，未成交部分不再挂单。
     * 若委托在成交过程中已因数量全部成交（{@code FILLED}），退款已由最后一笔成交的买方
     * 结算命令携带，本方法不会重复退款；重复调用同样幂等。
     */
    OrderTradeApplyResult completeMarketBuy(long orderId);

    /**
     * 撤单准备：查询委托并校验归属、交易对与可撤状态，返回撤单快照。
     *
     * <p>只允许撤 {@code PAID} / {@code PARTIALLY_FILLED}；{@code ACCEPTED} 仍在冻结回执途中，
     * 返回 {@code ORDER_NOT_PAID_YET}，由调用方提示稍后重试。{@code userId > 0} 时校验
     * 委托归属，0 表示命令未携带用户、跳过归属校验。
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
     * 撤单更新：把委托从可撤状态原子推进为 {@code CANCELED}，并按成交进度计算解冻金额，
     * 生成一条只含 {@code ORDER_UNFREEZE} 腿的结算命令。
     *
     * <p>解冻金额：买单为 {@code frozenAmount - settledQuoteAmount}，卖单为
     * {@code quantity - filledQuantity}；为 0 时不生成清算命令。委托在准备阶段之后
     * 已全部成交或已被撤销时返回 {@code handled=false}，不重复解冻。
     *
     * <p>本方法由撤单监听器的更新线程池并发调用，按 orderId 在
     * {@link java.util.concurrent.ConcurrentHashMap#computeIfPresent} 中原子推进状态。
     */
    OrderCancelApplyResult applyCancel(OrderCancelContext context);

    /**
     * 按委托号查询；委托不存在时返回 {@code null}。
     */
    OrderView view(long orderId);

    /**
     * 查询用户当前未完成的委托（{@code ACCEPTED} / {@code PAID} / {@code PARTIALLY_FILLED}），
     * 按创建时间倒序（最新在前）。已成交、已拒绝的委托不返回。
     *
     * @param userId 用户 ID，必须为正数
     * @param symbol 可选交易对筛选；{@code null} / 空白表示全部交易对
     * @param limit  1..200，超出范围按边界夹取
     */
    List<OrderView> openOrders(long userId, String symbol, int limit);
}
