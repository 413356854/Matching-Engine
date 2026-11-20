package com.me.admin.controller;

import com.me.admin.dto.AdminPairCreateRequest;
import com.me.admin.dto.AdminPairResult;
import com.me.admin.dto.AdminPairUpdateRequest;
import com.me.admin.service.AdminPairService;
import com.me.admin.vo.AdminPairQueryView;
import com.me.admin.vo.AdminPairView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端交易对配置接口。
 *
 * <p>{@code GET /api/v1/admin/pairs} 返回全部交易对（含下线），按展示顺序升序；
 * {@code POST} 新建、{@code PUT /{id}} 修改、{@code DELETE /{id}} 删除。
 * 写操作直接更新 MySQL {@code pair_symbol} 并刷新启动缓存，下单校验立即生效。
 *
 * <p>新建时 symbol 由 {@code baseCurrency + "-" + quoteCurrency} 派生；symbol 与币种创建后
 * 不可修改。删除只允许已下线的交易对，在线返回 409。
 *
 * <p>这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin/pairs")
public class AdminPairController {

    private static final Logger log = LoggerFactory.getLogger(AdminPairController.class);

    private final AdminPairService adminPairService;

    public AdminPairController(AdminPairService adminPairService) {
        this.adminPairService = adminPairService;
    }

    @GetMapping
    public ResponseEntity<AdminPairQueryView> list() {
        try {
            return ResponseEntity.ok(AdminPairQueryView.success(adminPairService.listPairs()));
        } catch (DataAccessException e) {
            log.error("admin pair list failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminPairQueryView.rejected("QUERY_FAILED"));
        }
    }

    @PostMapping
    public ResponseEntity<AdminPairView> create(
            @RequestBody(required = false) AdminPairCreateRequest request) {
        if (request == null) {
            return badRequest("REQUEST_REQUIRED");
        }
        try {
            AdminPairResult result = adminPairService.create(request);
            if (!result.success()) {
                return mutationFailure(result.reason());
            }
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(AdminPairView.success(result.pair()));
        } catch (DataAccessException e) {
            log.error("admin pair create failed: base={}, quote={}",
                    request.baseCurrency(), request.quoteCurrency(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminPairView.rejected("WRITE_FAILED"));
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<AdminPairView> update(
            @PathVariable("id") long id,
            @RequestBody(required = false) AdminPairUpdateRequest request) {
        if (id <= 0) {
            return badRequest("ID_INVALID");
        }
        if (request == null) {
            return badRequest("REQUEST_REQUIRED");
        }
        try {
            AdminPairResult result = adminPairService.update(id, request);
            if (!result.success()) {
                return mutationFailure(result.reason());
            }
            return ResponseEntity.ok(AdminPairView.success(result.pair()));
        } catch (DataAccessException e) {
            log.error("admin pair update failed: id={}", id, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminPairView.rejected("WRITE_FAILED"));
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<AdminPairView> delete(@PathVariable("id") long id) {
        if (id <= 0) {
            return badRequest("ID_INVALID");
        }
        try {
            AdminPairResult result = adminPairService.delete(id);
            if (!result.success()) {
                return mutationFailure(result.reason());
            }
            return ResponseEntity.ok(AdminPairView.success(null));
        } catch (DataAccessException e) {
            log.error("admin pair delete failed: id={}", id, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminPairView.rejected("WRITE_FAILED"));
        }
    }

    private static ResponseEntity<AdminPairView> mutationFailure(String reason) {
        HttpStatus status = switch (reason) {
            case "PAIR_NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "SYMBOL_ALREADY_EXISTS", "SYMBOL_BOOK_NOT_EMPTY",
                 "PAIR_ONLINE", "PAIR_HAS_OPEN_ORDERS" -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(AdminPairView.rejected(reason));
    }

    private static ResponseEntity<AdminPairView> badRequest(String reason) {
        return ResponseEntity.badRequest().body(AdminPairView.rejected(reason));
    }
}
