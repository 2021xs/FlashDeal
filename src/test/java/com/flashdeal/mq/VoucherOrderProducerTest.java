package com.flashdeal.mq;

import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderProducerTest {

    @Test
    void sendSeckillOrderShouldPublishPersistentMessage() throws Exception {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        IMqMessageService mqMessageService = mock(IMqMessageService.class);
        when(mqMessageService.markSent(100L)).thenReturn(true);
        VoucherOrderProducer producer = new VoucherOrderProducer();
        ReflectionTestUtils.setField(producer, "rabbitTemplate", rabbitTemplate);
        ReflectionTestUtils.setField(producer, "mqMessageService", mqMessageService);

        producer.sendSeckillOrder(message());

        ArgumentCaptor<MessagePostProcessor> postProcessorCaptor =
                ArgumentCaptor.forClass(MessagePostProcessor.class);
        verify(rabbitTemplate).convertAndSend(
                eq("flashdeal.seckill.order.exchange"),
                eq("flashdeal.seckill.order"),
                any(Object.class),
                postProcessorCaptor.capture(),
                any(CorrelationData.class)
        );
        MessageProperties properties = new MessageProperties();
        Message processed = postProcessorCaptor.getValue().postProcessMessage(new Message(new byte[0], properties));

        assertEquals(100L, processed.getMessageProperties().getHeaders().get("messageId"));
        assertEquals(MessageDeliveryMode.PERSISTENT, processed.getMessageProperties().getDeliveryMode());
    }

    private VoucherOrderMessage message() {
        VoucherOrderMessage message = new VoucherOrderMessage();
        message.setMessageId(100L);
        message.setOrderId(200L);
        message.setUserId(300L);
        message.setVoucherId(400L);
        return message;
    }
}
