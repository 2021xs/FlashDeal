package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderCloseDeadLetterConsumerTest {

    @Test
    void deadLetterShouldRecordFailureAndAck() throws Exception {
        OrderCloseDeadLetterConsumer consumer = new OrderCloseDeadLetterConsumer();
        IOrderTimeoutCloseFailService failService = mock(IOrderTimeoutCloseFailService.class);
        Channel channel = mock(Channel.class);
        Message message = message(100L);
        OrderTimeoutMessage timeoutMessage = timeoutMessage();
        when(failService.recordFailureWithBackoff(
                eq(10L), eq(20L), eq(30L), startsWith("ORDER_CLOSE_DLQ"), eq(3), eq(60L), eq(3600L)))
                .thenReturn(failEvent());
        ReflectionTestUtils.setField(consumer, "orderTimeoutCloseFailService", failService);
        ReflectionTestUtils.setField(consumer, "maxFailCount", 3);
        ReflectionTestUtils.setField(consumer, "baseRetryDelaySeconds", 60L);
        ReflectionTestUtils.setField(consumer, "maxRetryDelaySeconds", 3600L);

        consumer.handleOrderCloseDeadLetter(timeoutMessage, message, channel);

        verify(failService).recordFailureWithBackoff(
                eq(10L), eq(20L), eq(30L), startsWith("ORDER_CLOSE_DLQ"), eq(3), eq(60L), eq(3600L));
        verify(channel).basicAck(100L, false);
    }

    @Test
    void failureRecordErrorShouldPropagateToDlqRetryContainer() throws Exception {
        OrderCloseDeadLetterConsumer consumer = new OrderCloseDeadLetterConsumer();
        IOrderTimeoutCloseFailService failService = mock(IOrderTimeoutCloseFailService.class);
        Channel channel = mock(Channel.class);
        doThrow(new IllegalStateException("mysql down")).when(failService).recordFailureWithBackoff(
                eq(10L), eq(20L), eq(30L), startsWith("ORDER_CLOSE_DLQ"), eq(3), eq(60L), eq(3600L));
        ReflectionTestUtils.setField(consumer, "orderTimeoutCloseFailService", failService);
        ReflectionTestUtils.setField(consumer, "maxFailCount", 3);
        ReflectionTestUtils.setField(consumer, "baseRetryDelaySeconds", 60L);
        ReflectionTestUtils.setField(consumer, "maxRetryDelaySeconds", 3600L);

        assertThrows(IllegalStateException.class,
                () -> consumer.handleOrderCloseDeadLetter(timeoutMessage(), message(100L), channel));

        verify(channel, org.mockito.Mockito.never()).basicAck(100L, false);
    }

    private static OrderTimeoutMessage timeoutMessage() {
        OrderTimeoutMessage timeoutMessage = new OrderTimeoutMessage();
        timeoutMessage.setOrderId(10L);
        timeoutMessage.setUserId(20L);
        timeoutMessage.setVoucherId(30L);
        return timeoutMessage;
    }

    private static Message message(long deliveryTag) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        properties.setHeader("x-death", "test");
        return new Message("{}".getBytes(StandardCharsets.UTF_8), properties);
    }

    private static OrderTimeoutCloseFail failEvent() {
        OrderTimeoutCloseFail fail = new OrderTimeoutCloseFail();
        fail.setOrderId(10L);
        fail.setUserId(20L);
        fail.setVoucherId(30L);
        fail.setFailCount(1);
        fail.setStatus(IOrderTimeoutCloseFailService.STATUS_INIT);
        return fail;
    }
}
