package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_QUEUE;

@Slf4j
@Component
public class OrderCloseConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = ORDER_CLOSE_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void handleOrderClose(OrderTimeoutMessage timeoutMessage, Message message, Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        log.info("Order close message received, orderId={}", timeoutMessage.getOrderId());
        voucherOrderService.closeTimeoutOrder(timeoutMessage.getOrderId());
        channel.basicAck(deliveryTag, false);
        log.info("Order close message consumed, orderId={}", timeoutMessage.getOrderId());
    }
}
