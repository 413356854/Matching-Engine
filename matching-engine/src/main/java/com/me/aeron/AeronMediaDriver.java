package com.me.aeron;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import java.nio.file.Path;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Aeron media driver 生命周期：当前与 Spring Boot 同进程启动内嵌 driver，
 * 供 WS 交易端点和币币 ingress 通过 {@code aeron:ipc} 通信。
 *
 * <p>phase=55：晚于下单 / 撤单 ingress ring（50），早于 Aeron 消费者（59/60）和
 * Web 服务器（{@code Integer.MAX_VALUE - 1}）。停止顺序相反，先停消费者再停 driver。
 *
 * <p>{@code embedded-driver=false} 时只连接 {@code app.aeron.dir} 指向的已有 driver，
 * 用于后续 WS 独立部署或多进程共享 driver。
 */
public final class AeronMediaDriver implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AeronMediaDriver.class);

    private final AeronProperties properties;

    private volatile MediaDriver mediaDriver;
    private volatile Aeron aeron;
    private volatile boolean running;

    public AeronMediaDriver(AeronProperties properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        String aeronDir = Path.of(properties.dir()).toAbsolutePath().normalize().toString();
        try {
            if (properties.embeddedDriver()) {
                MediaDriver.Context context = new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .dirDeleteOnStart(properties.dirDeleteOnStart())
                        .dirDeleteOnShutdown(false)
                        .ipcTermBufferLength(properties.termBufferLength())
                        .publicationTermBufferLength(properties.termBufferLength());
                mediaDriver = MediaDriver.launch(context);
                log.debug("aeron media driver started: dir={}, ipcTermBuffer={}",
                        aeronDir, properties.termBufferLength());
            } else {
                log.debug("aeron media driver external: dir={}", aeronDir);
            }

            Aeron.Context aeronContext = new Aeron.Context()
                    .aeronDirectoryName(aeronDir);
            aeron = Aeron.connect(aeronContext);
            running = true;
        } catch (RuntimeException e) {
            closeResources();
            log.error("failed to start aeron media driver: dir={}", aeronDir, e);
            throw new IllegalStateException("failed to start aeron media driver", e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        closeResources();
        log.debug("aeron media driver stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 55;
    }

    /** 连接后的 Aeron client；必须在 {@link #start()} 之后调用。 */
    public Aeron aeron() {
        Aeron current = aeron;
        if (current == null) {
            throw new IllegalStateException("aeron client is not started");
        }
        return current;
    }

    private void closeResources() {
        CloseHelper.quietClose(aeron);
        aeron = null;
        CloseHelper.quietClose(mediaDriver);
        mediaDriver = null;
    }
}
