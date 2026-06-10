package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.service.IOutboxEventService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderTimeoutOutboxPublishTaskTest {

    @Test
    void publishSuccessShouldMarkSentAndUseRemainingDelay() throws Exception {
        Fixture fixture = new Fixture();
        OutboxEvent event = fixture.event(LocalDateTime.now().plusSeconds(60));
        when(fixture.outboxEventService.claimSending(event)).thenReturn(true);

        fixture.task.publishOne(event);

        verify(fixture.orderTimeoutProducer).sendOrderTimeoutMessage(
                any(OrderTimeoutMessage.class), eq(event.getExchangeName()), eq(event.getRoutingKey()), any(Long.class));
        verify(fixture.outboxEventService).markSent(event);
        verify(fixture.outboxEventService, never()).markPublishFailed(any(), any(), any());
    }

    @Test
    void publishFailureShouldRecordRetryInformation() throws Exception {
        Fixture fixture = new Fixture();
        OutboxEvent event = fixture.event(LocalDateTime.now().plusSeconds(60));
        when(fixture.outboxEventService.claimSending(event)).thenReturn(true);
        doThrow(new IllegalStateException("rabbit down")).when(fixture.orderTimeoutProducer)
                .sendOrderTimeoutMessage(any(), any(), any(), any(Long.class));

        fixture.task.publishOne(event);

        verify(fixture.outboxEventService).markPublishFailed(eq(event), any(LocalDateTime.class), any());
        verify(fixture.outboxEventService, never()).markSent(event);
    }

    @Test
    void claimFailureShouldPreventDuplicatePublish() throws Exception {
        Fixture fixture = new Fixture();
        OutboxEvent event = fixture.event(LocalDateTime.now().plusSeconds(60));
        when(fixture.outboxEventService.claimSending(event)).thenReturn(false);

        fixture.task.publishOne(event);

        verify(fixture.orderTimeoutProducer, never()).sendOrderTimeoutMessage(any(), any(), any(), any(Long.class));
    }

    @Test
    void remainingDelayShouldNeverMoveTimeoutBackwards() {
        Fixture fixture = new Fixture();
        LocalDateTime now = LocalDateTime.now();

        long remaining = fixture.task.calculateRemainingDelayMillis(now.plusSeconds(30), now);
        long expired = fixture.task.calculateRemainingDelayMillis(now.minusSeconds(1), now);

        assertTrue(remaining >= 29_000L && remaining <= 30_000L);
        assertEquals(0L, expired);
    }

    private static class Fixture {
        private final OrderTimeoutOutboxPublishTask task = new OrderTimeoutOutboxPublishTask();
        private final IOutboxEventService outboxEventService = mock(IOutboxEventService.class);
        private final OrderTimeoutProducer orderTimeoutProducer = mock(OrderTimeoutProducer.class);
        private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

        private Fixture() {
            ReflectionTestUtils.setField(task, "outboxEventService", outboxEventService);
            ReflectionTestUtils.setField(task, "orderTimeoutProducer", orderTimeoutProducer);
            ReflectionTestUtils.setField(task, "objectMapper", objectMapper);
            ReflectionTestUtils.setField(task, "retryBaseDelaySeconds", 10L);
            ReflectionTestUtils.setField(task, "retryMaxDelaySeconds", 300L);
            ReflectionTestUtils.setField(task, "batchSize", 100);
        }

        private OutboxEvent event(LocalDateTime expireTime) throws Exception {
            OrderTimeoutMessage message = new OrderTimeoutMessage();
            message.setOrderId(10L);
            message.setUserId(20L);
            message.setVoucherId(30L);
            message.setCreateTime(LocalDateTime.now());
            message.setExpireTime(expireTime);
            return new OutboxEvent()
                    .setId(10L)
                    .setEventId("ORDER_TIMEOUT:10")
                    .setEventType("ORDER_TIMEOUT")
                    .setBizKey("order:10")
                    .setExchangeName("flashdeal.order.timeout.exchange")
                    .setRoutingKey("flashdeal.order.timeout")
                    .setPayload(objectMapper.writeValueAsString(message))
                    .setStatus("INIT")
                    .setRetryCount(0)
                    .setMaxRetryCount(5)
                    .setExpireTime(expireTime);
        }
    }
}
