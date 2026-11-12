package com.me.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 显式声明 MySQL 主数据源。
 *
 * <p>admin 模块为 QuestDB 注册了独立的 DataSource Bean；Spring Boot 的
 * {@code DataSourceAutoConfiguration} 在发现任意 DataSource Bean 后会退避。
 * 这里把原有 {@code spring.datasource.*} 配置重新声明为 {@code @Primary}，
 * 保证 MyBatis-Plus、事务和现有连接测试仍使用 MySQL。
 */
@Configuration
public class MySqlDataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.driver-class-name}") String driverClassName) {
        // 使用无参构造，让 @ConfigurationProperties 在连接池启动前完成 pool-name 等属性绑定。
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClassName);
        return dataSource;
    }
}
