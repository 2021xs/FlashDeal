package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.service.IOutboxEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class OrderTimeoutOutboxPublishTask {

    @Resource
    private IOutboxEventService outboxEventService;

    @Resource
    private OrderTimeoutProducer orderTimeoutProducer;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${outbox.order-timeout.enabled:true}")
    private boolean enabled;

    @Value("${outbox.order-timeout.batch-size:100}")
    private int batchSize;

    @Value("${outbox.order-timeout.sending-timeout-seconds:60}")
    private long sendingTimeoutSeconds;

    @Value("${outbox.order-timeout.retry-base-delay-seconds:10}")
    private long retryBaseDelaySeconds;

    @Value("${outbox.order-timeout.retry-max-delay-seconds:300}")
    private long retryMaxDelaySeconds;

    @Scheduled(fixedDelayString = "#{${outbox.order-timeout.publish-interval-seconds:5} * 1000}")
    public void publishOrderTimeoutEvents() {
        if (!enabled) {
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        outboxEventService.recoverStuckSending(
                now.minusSeconds(Math.max(1L, sendingTimeoutSeconds)), now, safeBatchSize());
        List<OutboxEvent> events = outboxEventService.listPublishable(now, safeBatchSize());
        for (OutboxEvent event : events) {
            publishOne(event);
        }
    }

    void publishOne(OutboxEvent event) {
        if (!outboxEventService.claimSending(event)) {
            log.debug("Skip outbox event because claim failed, eventId={}, status={}, retryCount={}",
                    event.getEventId(), event.getStatus(), event.getRetryCount());
            return;
        }
        try {
            OrderTimeoutMessage message = objectMapper.readValue(event.getPayload(), OrderTimeoutMessage.class);
            long remainingDelayMillis = calculateRemainingDelayMillis(event.getExpireTime(), LocalDateTime.now());
            orderTimeoutProducer.sendOrderTimeoutMessage(
                    message, event.getExchangeName(), event.getRoutingKey(), remainingDelayMillis, event.getEventId());
            if (!outboxEventService.markSent(event)) {
                log.error("Mark order timeout outbox event SENT failed, eventId={}, orderId={}",
                        event.getEventId(), message.getOrderId());
            }
        } catch (Exception e) {
            int nextRetryCount = safeRetryCount(event) + 1;
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(calculateBackoffSeconds(nextRetryCount));
            boolean updated = outboxEventService.markPublishFailed(
                    event, nextRetryTime, summarizeException(e));
            log.error("Publish order timeout outbox event failed, eventId={}, bizKey={}, retryCount={}, updated={}",
                    event.getEventId(), event.getBizKey(), nextRetryCount, updated, e);
        }
    }

    long calculateRemainingDelayMillis(LocalDateTime expireTime, LocalDateTime now) {
        if (expireTime == null || now == null) {
            return 0L;
        }
        return Math.max(0L, Duration.between(now, expireTime).toMillis());
    }

    private long calculateBackoffSeconds(int retryCount) {
        long base = Math.max(1L, retryBaseDelaySeconds);
        long max = Math.max(base, retryMaxDelaySeconds);
        long delay = base;
        for (int i = 1; i < retryCount && delay < max; i++) {
            delay = Math.min(max, delay * 2);
        }
        return delay;
    }

    private int safeRetryCount(OutboxEvent event) {
        return event.getRetryCount() == null ? 0 : event.getRetryCount();
    }

    private int safeBatchSize() {
        return Math.max(1, batchSize);
    }

    private String summarizeException(Throwable e) {
        String message = e.getMessage();
        String summary = e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return summary.length() <= 512 ? summary : summary.substring(0, 512);
    }
}
