package com.flashdeal.mq;

import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLQ;

@Slf4j
@Component
public class OrderCloseDeadLetterConsumer {

    @Resource
    private IOrderTimeoutCloseFailService orderTimeoutCloseFailService;

    @Value("${order-timeout.close-dlq.max-fail-count:3}")
    private Integer maxFailCount;

    @Value("${order-timeout.close-retry.base-delay-seconds:60}")
    private long baseRetryDelaySeconds;

    @Value("${order-timeout.close-retry.max-delay-seconds:3600}")
    private long maxRetryDelaySeconds;

    @RabbitListener(queues = ORDER_CLOSE_DLQ, containerFactory = "dlqRabbitListenerContainerFactory")
    public void handleOrderCloseDeadLetter(
            OrderTimeoutMessage timeoutMessage,
            Message message,
            Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        OrderTimeoutCloseFail fail = null;
        if (timeoutMessage != null && timeoutMessage.getOrderId() != null) {
            fail = orderTimeoutCloseFailService.recordFailureWithBackoff(
                    timeoutMessage.getOrderId(),
                    timeoutMessage.getUserId(),
                    timeoutMessage.getVoucherId(),
                    "ORDER_CLOSE_DLQ headers=" + headers,
                    safeMaxFailCount(),
                    baseRetryDelaySeconds,
                    maxRetryDelaySeconds);
        }
        log.error("Order close dead letter received, orderId={}, userId={}, voucherId={}, failHeaders={}",
                timeoutMessage == null ? null : timeoutMessage.getOrderId(),
                timeoutMessage == null ? null : timeoutMessage.getUserId(),
                timeoutMessage == null ? null : timeoutMessage.getVoucherId(),
                headers);
        if (fail != null && IOrderTimeoutCloseFailService.STATUS_NEED_MANUAL.equals(fail.getStatus())) {
            log.error("Order close dead letter reached NEED_MANUAL, orderId={}, userId={}, voucherId={}, failCount={}",
                    timeoutMessage.getOrderId(), timeoutMessage.getUserId(), timeoutMessage.getVoucherId(), fail.getFailCount());
        }
        channel.basicAck(deliveryTag, false);
    }

    private int safeMaxFailCount() {
        return maxFailCount == null || maxFailCount <= 0 ? 3 : maxFailCount;
    }
}
