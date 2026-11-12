package com.me.admin.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * QuestDB 独立数据源与 JdbcTemplate，不占用 MySQL 默认 DataSource。
 *
 * <p>QuestDB 通过 PostgreSQL wire protocol 访问（默认 {@code jdbc:postgresql://localhost:8812/qdb}，
 * 用户名 {@code admin}、密码 {@code quest}）。
 */
@Configuration
@EnableConfigurationProperties(QuestDbProperties.class)
public class QuestDbConfig {

    private static final Logger log = LoggerFactory.getLogger(QuestDbConfig.class);

    @Bean(name = "questDbDataSource", destroyMethod = "close")
    public HikariDataSource questDbDataSource(QuestDbProperties properties) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("questdb-pool");
        config.setJdbcUrl(properties.jdbcUrl());
        config.setUsername(properties.username());
        config.setPassword(properties.password());
        config.setMaximumPoolSize(properties.poolSize());
        config.setMinimumIdle(1);
        config.setConnectionTimeout(properties.connectionTimeoutMs());
        // QuestDB 短暂不可用时不阻塞应用启动，首次写入再重试连接。
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    @Bean(name = "questDbJdbcTemplate")
    public JdbcTemplate questDbJdbcTemplate(@Qualifier("questDbDataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        try {
            jdbcTemplate.execute(QuestDbSchema.CREATE_FUNDS_FLOW_TABLE);
        } catch (Exception e) {
            log.warn("QuestDB table init failed; will fail on first write until QuestDB is available: {}",
                    e.getMessage());
        }
        return jdbcTemplate;
    }
}
