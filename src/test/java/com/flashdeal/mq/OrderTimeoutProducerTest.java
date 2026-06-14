package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

class OrderTimeoutProducerTest {

    @Test
    void delayedPublishShouldUseRemainingDelayAsMessageTtl() throws Exception {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        OrderTimeoutProducer producer = new OrderTimeoutProducer();
        ReflectionTestUtils.setField(producer, "rabbitTemplate", rabbitTemplate);
        ReflectionTestUtils.setField(producer, "confirmTimeoutSeconds", 1L);
        confirmPublishedMessages(rabbitTemplate);
        ArgumentCaptor<MessagePostProcessor> processorCaptor = ArgumentCaptor.forClass(MessagePostProcessor.class);

        producer.sendOrderTimeoutMessage(message(), "timeout.exchange", "timeout.key", 12_345L, "event-1");

        verify(rabbitTemplate).convertAndSend(
                eq("timeout.exchange"), eq("timeout.key"), any(OrderTimeoutMessage.class),
                processorCaptor.capture(), any(CorrelationData.class));
        Message processed = processorCaptor.getValue().postProcessMessage(
                new Message(new byte[0], new MessageProperties()));
        assertEquals("12345", processed.getMessageProperties().getExpiration());
    }

    @Test
    void expiredEventShouldPublishDirectlyToCloseExchange() throws Exception {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        OrderTimeoutProducer producer = new OrderTimeoutProducer();
        ReflectionTestUtils.setField(producer, "rabbitTemplate", rabbitTemplate);
        ReflectionTestUtils.setField(producer, "confirmTimeoutSeconds", 1L);
        confirmPublishedMessages(rabbitTemplate);

        producer.sendOrderTimeoutMessage(message(), "timeout.exchange", "timeout.key", 0L, "event-1");

        verify(rabbitTemplate).convertAndSend(
                eq("flashdeal.order.close.exchange"), eq("flashdeal.order.close"), any(OrderTimeoutMessage.class),
                any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    private OrderTimeoutMessage message() {
        OrderTimeoutMessage message = new OrderTimeoutMessage();
        message.setOrderId(10L);
        return message;
    }

    private void confirmPublishedMessages(RabbitTemplate rabbitTemplate) {
        doAnswer(invocation -> {
            CorrelationData correlationData = invocation.getArgument(4);
            correlationData.getFuture().set(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(
                any(String.class), any(String.class), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
    }
}
