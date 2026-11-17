package com.me.snapshot;

import com.me.kafka.KafkaClientProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 备份快照的配置装配。
 *
 * <p>快照需要查询消费组 committed offset 与 topic end offset，因此单独创建一个
 * 长生命周期的 {@link AdminClient}；它不参与消费，关闭时由 Spring 调用 {@code close()}。
 */
@Configuration
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(SnapshotProperties.class)
public class SnapshotConfiguration {

    @Bean(destroyMethod = "close")
    public AdminClient snapshotAdminClient(KafkaClientProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG,
                "snapshot-admin-" + properties.instanceId());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return AdminClient.create(config);
    }
}
