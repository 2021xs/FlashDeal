package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_ROUTING_KEY;

@Slf4j
@Component
public class OrderTimeoutProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Value("${outbox.order-timeout.confirm-timeout-seconds:5}")
    private long confirmTimeoutSeconds;

    public void sendOrderTimeoutMessage(OrderTimeoutMessage message,
                                        String exchange,
                                        String routingKey,
                                        long remainingDelayMillis,
                                        String eventId) throws Exception {
        long messageTtl = Math.max(1L, remainingDelayMillis);
        CorrelationData correlationData = new CorrelationData("outbox:" + eventId);
        if (remainingDelayMillis <= 0) {
            rabbitTemplate.convertAndSend(ORDER_CLOSE_EXCHANGE, ORDER_CLOSE_ROUTING_KEY, message,
                    amqpMessage -> {
                        amqpMessage.getMessageProperties().setHeader("outboxEventId", eventId);
                        return amqpMessage;
                    }, correlationData);
            awaitConfirm(correlationData);
            log.info("Expired order timeout message sent directly to close exchange, orderId={}", message.getOrderId());
            return;
        }
        rabbitTemplate.convertAndSend(
                exchange,
                routingKey,
                message,
                amqpMessage -> {
                    amqpMessage.getMessageProperties().setExpiration(String.valueOf(messageTtl));
                    amqpMessage.getMessageProperties().setHeader("outboxEventId", eventId);
                    return amqpMessage;
                },
                correlationData
        );
        awaitConfirm(correlationData);
        log.info("Order timeout message sent, orderId={}, remainingDelayMillis={}",
                message.getOrderId(), remainingDelayMillis);
    }

    private void awaitConfirm(CorrelationData correlationData) throws Exception {
        CorrelationData.Confirm confirm = correlationData.getFuture()
                .get(Math.max(1L, confirmTimeoutSeconds), TimeUnit.SECONDS);
        if (!confirm.isAck()) {
            throw new IllegalStateException("Order timeout message publisher confirm NACK: " + confirm.getReason());
        }
        if (correlationData.getReturnedMessage() != null) {
            throw new IllegalStateException("Order timeout message was returned: "
                    + correlationData.getReturnedMessage());
        }
    }
}
