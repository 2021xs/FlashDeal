package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OrderCloseConsumerTest {

    @Test
    void closeFailureShouldPropagateToContainerRetryWithoutManualNack() throws Exception {
        OrderCloseConsumer consumer = new OrderCloseConsumer();
        IVoucherOrderService orderService = mock(IVoucherOrderService.class);
        Channel channel = mock(Channel.class);
        ReflectionTestUtils.setField(consumer, "voucherOrderService", orderService);
        doThrow(new IllegalStateException("mysql down")).when(orderService).closeTimeoutOrder(10L);

        assertThrows(IllegalStateException.class,
                () -> consumer.handleOrderClose(timeoutMessage(), rawMessage(100L), channel));

        verify(channel, never()).basicAck(100L, false);
        verify(channel, never()).basicNack(100L, false, false);
    }

    private OrderTimeoutMessage timeoutMessage() {
        OrderTimeoutMessage message = new OrderTimeoutMessage();
        message.setOrderId(10L);
        return message;
    }

    private Message rawMessage(long deliveryTag) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(new byte[0], properties);
    }
}
