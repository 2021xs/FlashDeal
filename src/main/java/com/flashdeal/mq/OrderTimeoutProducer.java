package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_ROUTING_KEY;

@Slf4j
@Component
public class OrderTimeoutProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    public void sendOrderTimeoutMessage(OrderTimeoutMessage message,
                                        String exchange,
                                        String routingKey,
                                        long remainingDelayMillis) {
        long messageTtl = Math.max(1L, remainingDelayMillis);
        if (remainingDelayMillis <= 0) {
            rabbitTemplate.convertAndSend(ORDER_CLOSE_EXCHANGE, ORDER_CLOSE_ROUTING_KEY, message);
            log.info("Expired order timeout message sent directly to close exchange, orderId={}", message.getOrderId());
            return;
        }
        rabbitTemplate.convertAndSend(
                exchange,
                routingKey,
                message,
                amqpMessage -> {
                    amqpMessage.getMessageProperties().setExpiration(String.valueOf(messageTtl));
                    return amqpMessage;
                }
        );
        log.info("Order timeout message sent, orderId={}, remainingDelayMillis={}",
                message.getOrderId(), remainingDelayMillis);
    }
}
