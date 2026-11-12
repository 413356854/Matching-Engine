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

    public static void main(String[] args) {
        SpringApplication.run(MeApplication.class, args);
    }
}
