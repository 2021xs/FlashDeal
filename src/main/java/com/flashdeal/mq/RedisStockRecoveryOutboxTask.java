package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.RedisStockRecoveryMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.service.IOutboxEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class RedisStockRecoveryOutboxTask {

    private static final DefaultRedisScript<Long> RECOVER_SCRIPT = recoverScript();

    @Resource
    private IOutboxEventService outboxEventService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${outbox.redis-stock-recovery.enabled:true}")
    private boolean enabled;

    @Value("${outbox.redis-stock-recovery.batch-size:100}")
    private int batchSize;

    @Value("${outbox.redis-stock-recovery.sending-timeout-seconds:60}")
    private long sendingTimeoutSeconds;

    @Value("${outbox.redis-stock-recovery.retry-delay-seconds:60}")
    private long retryDelaySeconds;

    @Scheduled(fixedDelayString = "#{${outbox.redis-stock-recovery.publish-interval-seconds:10} * 1000}")
    public void recoverRedisStock() {
        if (!enabled) {
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        outboxEventService.recoverStuckRedisStockRecovery(
                now.minusSeconds(Math.max(1L, sendingTimeoutSeconds)), now, safeBatchSize());
        List<OutboxEvent> events = outboxEventService.listRedisStockRecoveryPublishable(now, safeBatchSize());
        for (OutboxEvent event : events) {
            recoverOne(event);
        }
    }

    void recoverOne(OutboxEvent event) {
        if (!outboxEventService.claimSending(event)) {
            return;
        }
        try {
            RedisStockRecoveryMessage message =
                    objectMapper.readValue(event.getPayload(), RedisStockRecoveryMessage.class);
            Long result = stringRedisTemplate.execute(
                    RECOVER_SCRIPT,
                    Collections.emptyList(),
                    message.getVoucherId().toString(),
                    message.getOrderId().toString());
            if (result == null || result == 2L) {
                throw new IllegalStateException("Redis stock key is missing, voucherId=" + message.getVoucherId());
            }
            if (!outboxEventService.markSent(event)) {
                log.error("Mark Redis stock recovery outbox event SENT failed, eventId={}, orderId={}",
                        event.getEventId(), message.getOrderId());
            }
        } catch (Exception e) {
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(Math.max(1L, retryDelaySeconds));
            boolean updated = outboxEventService.markPublishFailed(event, nextRetryTime, summarizeException(e));
            log.error("Redis stock recovery outbox event failed, eventId={}, bizKey={}, updated={}",
                    event.getEventId(), event.getBizKey(), updated, e);
        }
    }

    private int safeBatchSize() {
        return Math.max(1, batchSize);
    }

    private String summarizeException(Throwable e) {
        String message = e.getMessage();
        String summary = e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return summary.length() <= 512 ? summary : summary.substring(0, 512);
    }

    private static DefaultRedisScript<Long> recoverScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis_stock_recover.lua"));
        script.setResultType(Long.class);
        return script;
    }
}
