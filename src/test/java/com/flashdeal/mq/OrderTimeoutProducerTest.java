package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderTimeoutProducerTest {

    @Test
    void delayedPublishShouldUseRemainingDelayAsMessageTtl() throws Exception {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        OrderTimeoutProducer producer = new OrderTimeoutProducer();
        ReflectionTestUtils.setField(producer, "rabbitTemplate", rabbitTemplate);
        ArgumentCaptor<MessagePostProcessor> processorCaptor = ArgumentCaptor.forClass(MessagePostProcessor.class);

        producer.sendOrderTimeoutMessage(message(), "timeout.exchange", "timeout.key", 12_345L);

        verify(rabbitTemplate).convertAndSend(
                eq("timeout.exchange"), eq("timeout.key"), any(OrderTimeoutMessage.class), processorCaptor.capture());
        Message processed = processorCaptor.getValue().postProcessMessage(
                new Message(new byte[0], new MessageProperties()));
        assertEquals("12345", processed.getMessageProperties().getExpiration());
    }

    @Test
    void expiredEventShouldPublishDirectlyToCloseExchange() {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        OrderTimeoutProducer producer = new OrderTimeoutProducer();
        ReflectionTestUtils.setField(producer, "rabbitTemplate", rabbitTemplate);

        producer.sendOrderTimeoutMessage(message(), "timeout.exchange", "timeout.key", 0L);

        verify(rabbitTemplate).convertAndSend(
                eq("flashdeal.order.close.exchange"), eq("flashdeal.order.close"), any(OrderTimeoutMessage.class));
    }

    private OrderTimeoutMessage message() {
        OrderTimeoutMessage message = new OrderTimeoutMessage();
        message.setOrderId(10L);
        return message;
    }
}
