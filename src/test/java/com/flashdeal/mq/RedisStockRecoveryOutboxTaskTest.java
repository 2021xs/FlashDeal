package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.RedisStockRecoveryMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.service.IOutboxEventService;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisStockRecoveryOutboxTaskTest {

    @Test
    void successfulOrAlreadyAppliedRecoveryShouldMarkEventSent() throws Exception {
        Fixture fixture = new Fixture();
        OutboxEvent event = fixture.event();
        when(fixture.outboxEventService.claimSending(event)).thenReturn(true);
        when(fixture.redisTemplate.execute(any(), anyList(), eq("30"), eq("10"))).thenReturn(1L);

        fixture.task.recoverOne(event);

        verify(fixture.outboxEventService).markSent(event);
        verify(fixture.outboxEventService, never()).markPublishFailed(any(), any(), any());
    }

    @Test
    void missingStockKeyShouldScheduleRetry() throws Exception {
        Fixture fixture = new Fixture();
        OutboxEvent event = fixture.event();
        when(fixture.outboxEventService.claimSending(event)).thenReturn(true);
        when(fixture.redisTemplate.execute(any(), anyList(), eq("30"), eq("10"))).thenReturn(2L);

        fixture.task.recoverOne(event);

        ArgumentCaptor<LocalDateTime> nextRetryCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(fixture.outboxEventService).markPublishFailed(eq(event), nextRetryCaptor.capture(), any());
        long delaySeconds = java.time.Duration.between(LocalDateTime.now(), nextRetryCaptor.getValue()).getSeconds();
        assertTrue(delaySeconds >= 119L && delaySeconds <= 121L);
        verify(fixture.outboxEventService, never()).markSent(event);
    }

    private static class Fixture {
        private final RedisStockRecoveryOutboxTask task = new RedisStockRecoveryOutboxTask();
        private final IOutboxEventService outboxEventService = mock(IOutboxEventService.class);
        private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        private final ObjectMapper objectMapper = new ObjectMapper();

        private Fixture() {
            ReflectionTestUtils.setField(task, "outboxEventService", outboxEventService);
            ReflectionTestUtils.setField(task, "stringRedisTemplate", redisTemplate);
            ReflectionTestUtils.setField(task, "objectMapper", objectMapper);
            ReflectionTestUtils.setField(task, "retryBaseDelaySeconds", 60L);
            ReflectionTestUtils.setField(task, "retryMaxDelaySeconds", 3600L);
        }

        private OutboxEvent event() throws Exception {
            RedisStockRecoveryMessage message = new RedisStockRecoveryMessage();
            message.setOrderId(10L);
            message.setUserId(20L);
            message.setVoucherId(30L);
            return new OutboxEvent()
                    .setId(1L)
                    .setEventId("event-1")
                    .setEventType("REDIS_STOCK_RECOVERY")
                    .setBizKey("order:10")
                    .setStatus("INIT")
                    .setRetryCount(1)
                    .setPayload(objectMapper.writeValueAsString(message));
        }
    }
}
