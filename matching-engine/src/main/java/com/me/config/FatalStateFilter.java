package com.me.config;

import com.me.kafka.FatalState;
import com.me.snapshot.recovery.RecoveryState;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * fatal / 启动恢复期间拒绝 HTTP 请求。
 *
 * <p>任一 consumer 失败后，内存状态可能已经不一致；此时所有 consumer 已停止，
 * 进程在等待外部监控重启。本过滤器保证这段时间内不再对外提供余额、委托、行情等
 * 可能已脏的数据，统一返回 503，让调用方和监控都能识别“引擎已停止，等待重启”。
 *
 * <p>启动快照恢复期间同样返回 503（恢复状态接口除外），避免外部读到恢复中的半状态。
 *
 * <p>进程重启后 {@link FatalState} 随 JVM 重建，过滤器自然恢复放行。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class FatalStateFilter extends OncePerRequestFilter {

    private static final String RECOVERY_STATUS_PATH = "/api/v1/admin/snapshots/recovery";

    private final FatalState fatalState;
    private final RecoveryState recoveryState;

    public FatalStateFilter(FatalState fatalState, RecoveryState recoveryState) {
        this.fatalState = fatalState;
        this.recoveryState = recoveryState;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        if (fatalState.isFatal()) {
            writeUnavailable(response,
                    "{\"code\":\"ENGINE_RESTART_REQUIRED\","
                            + "\"message\":\"matching engine stopped after a fatal consumer error; "
                            + "waiting for restart\"}");
            return;
        }
        if (!recoveryState.isReady() && !isRecoveryStatusRequest(request)) {
            writeUnavailable(response,
                    "{\"code\":\"RECOVERY_IN_PROGRESS\","
                            + "\"message\":\"matching engine is restoring from a snapshot; "
                            + "retry later\"}");
            return;
        }
        filterChain.doFilter(request, response);
    }

    private static boolean isRecoveryStatusRequest(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return RECOVERY_STATUS_PATH.equals(uri)
                || (RECOVERY_STATUS_PATH + "/").equals(uri);
    }

    private static void writeUnavailable(HttpServletResponse response, String body)
            throws IOException {
        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write(body);
    }
}
