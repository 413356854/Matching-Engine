package com.me;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 单体应用唯一启动类。
 *
 * <p>模块边界见 README：一个模块 = {@code com.me.<module>} 一个包，
 * 内部按 Java 习惯分层，Mapper 统一放在各模块的 {@code mapper} 包下。
 */
@SpringBootApplication
@MapperScan("com.me.**.mapper")
public class MeApplication {

    static {
        // 机器人模块访问币安时需要读取 Windows 系统代理。JVM 默认关闭该行为，
        // 且属性必须在 ProxySelector 首次初始化前设置（QuestDB 客户端等会创建
        // HttpClient）；放在启动类静态块，保证早于所有 Spring Bean。
        System.setProperty("java.net.useSystemProxies", "true");
    }

    public static void main(String[] args) {
        SpringApplication.run(MeApplication.class, args);
    }
}
