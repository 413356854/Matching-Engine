package com.me.order.service;

import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.vo.OrderView;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.event.TradeEvent;
import com.me.shared.order.OrderCommand;
import java.util.List;

/**
 * 委托业务接口：保存委托、处理冻结回执、成交清算、查询委托。跨模块只调用该接口。
 *
 * <p>下单命令只从 Kafka {@code me.order.commands-batch} 进入：{@link #apply(OrderCommand)} 由
 * {@code OrderCommandListener} 调用，是系统唯一的下单入口。
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
