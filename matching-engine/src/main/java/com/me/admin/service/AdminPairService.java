package com.me.admin.service;

import com.me.admin.dto.AdminPairCreateRequest;
import com.me.admin.dto.AdminPairResult;
import com.me.admin.dto.AdminPairUpdateRequest;
import com.me.currency.vo.TradingPairView;
import java.util.List;

/**
 * 管理端交易对配置的读写服务。
 *
 * <p>写操作成功后立即刷新 {@code CurrencyConfigCache}，新配置对下单接口生效，
 * 不需要重启应用。删除只允许已下线的交易对，避免误删仍在撮合的市场。
 */
public interface AdminPairService {

    /** 查询全部交易对（含下线），按 {@code sortOrder}、{@code id} 升序。 */
    List<TradingPairView> listPairs();

    /** 新建交易对：symbol 由基础币 / 计价币派生，初始状态默认为上线。 */
    AdminPairResult create(AdminPairCreateRequest request);

    /** 修改交易对的可变配置；symbol、币种与价格 / 数量精度不可修改。 */
    AdminPairResult update(long id, AdminPairUpdateRequest request);

    /** 删除已下线的交易对；仍在线或订单簿还有挂单时拒绝删除。 */
    AdminPairResult delete(long id);
}
