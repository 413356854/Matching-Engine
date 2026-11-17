package com.me.snapshot.recovery;

import com.me.snapshot.SnapshotRecoveryProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 恢复配置注册。
 *
 * <p>恢复状态 / HTTP 门禁在 {@code app.kafka.enabled=false} 时也要存在，因此这里不跟随
 * snapshot 主配置的条件。
 */
@Configuration
@EnableConfigurationProperties(SnapshotRecoveryProperties.class)
public class SnapshotRecoveryConfiguration {
}
