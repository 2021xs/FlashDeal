package com.flashdeal.mq;

import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;

import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_ROUTING_KEY;

@Slf4j
@Component
public class VoucherOrderProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private IMqMessageService mqMessageService;

    @Value("${seckill.mq-message.confirm-failed-next-retry-delay-seconds:30}")
    private Long confirmFailedNextRetryDelaySeconds;

    @Value("${seckill.mq-message.returned-next-retry-delay-seconds:30}")
    private Long returnedNextRetryDelaySeconds;

    @Value("${seckill.mq-message.send-failed-next-retry-delay-seconds:1}")
    private Long sendFailedNextRetryDelaySeconds;

    @PostConstruct
    public void initCallbacks() {
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (isInternalCorrelation(correlationData)) {
                return;
            }
            Long messageId = parseMessageId(correlationData == null ? null : correlationData.getId(), "confirm");
            if (messageId == null) {
                return;
            }
            if (ack) {
                boolean updated = mqMessageService.markConfirmed(messageId);
                if (!updated) {
                    log.warn("Seckill order message confirm ack status skipped, messageId={}", messageId);
                }
                return;
            }
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(confirmFailedNextRetryDelaySeconds);
            boolean updated = mqMessageService.markConfirmFailed(messageId, limitReason(cause), nextRetryTime);
            log.error("Seckill order message confirm failed, messageId={}, updated={}, cause={}",
                    messageId, updated, cause);
        });
        rabbitTemplate.setReturnCallback(this::handleReturnedMessage);
    }

    public void sendSeckillOrder(VoucherOrderMessage message) {
        try {
            doSendSeckillOrder(message);
        } catch (RuntimeException e) {
            Long messageId = message.getMessageId();
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(sendFailedNextRetryDelaySeconds);
            boolean updated = mqMessageService.markSendFailed(messageId, limitReason(e.getMessage()), nextRetryTime);
            log.error("Seckill order message send failed before convertAndSend returned, messageId={}, orderId={}, userId={}, voucherId={}, updated={}",
                    messageId, message.getOrderId(), message.getUserId(), message.getVoucherId(), updated, e);
            throw e;
        }
        Long messageId = message.getMessageId();
        boolean updated = mqMessageService.markSent(messageId);
        if (!updated) {
            log.error("Mark seckill mq message SENT failed after convertAndSend returned, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, message.getOrderId(), message.getUserId(), message.getVoucherId());
        }
    }

    public void resendSeckillOrder(VoucherOrderMessage message) {
        doSendSeckillOrder(message);
        Long messageId = message.getMessageId();
        boolean updated = mqMessageService.markSentForRetry(messageId);
        if (!updated) {
            log.error("Mark retried seckill mq message SENT failed after convertAndSend returned, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, message.getOrderId(), message.getUserId(), message.getVoucherId());
        }
    }

    private void doSendSeckillOrder(VoucherOrderMessage message) {
        Long messageId = message.getMessageId();
        CorrelationData correlationData = new CorrelationData(String.valueOf(messageId));
        rabbitTemplate.convertAndSend(
                SECKILL_ORDER_EXCHANGE,
                SECKILL_ORDER_ROUTING_KEY,
                message,
                amqpMessage -> {
                    amqpMessage.getMessageProperties().setHeader("messageId", messageId);
                    amqpMessage.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return amqpMessage;
                },
                correlationData
        );
    }

    private void handleReturnedMessage(
            Message message,
            int replyCode,
            String replyText,
            String exchange,
            String routingKey) {
        if (message.getMessageProperties().getHeaders().containsKey("outboxEventId")) {
            log.error("Order timeout outbox message returned, outboxEventId={}, exchange={}, routingKey={}, replyCode={}, replyText={}",
                    message.getMessageProperties().getHeaders().get("outboxEventId"),
                    exchange, routingKey, replyCode, replyText);
            return;
        }
        if (message.getMessageProperties().getHeaders().containsKey(VoucherOrderConsumer.CLAIM_TRANSFER_HEADER)) {
            log.error("Seckill claim transfer message returned, exchange={}, routingKey={}, replyCode={}, replyText={}, body={}",
                    exchange, routingKey, replyCode, replyText, message);
            return;
        }
        Long messageId = parseMessageId(message.getMessageProperties().getHeaders().get("messageId"), "return");
        if (messageId == null) {
            log.error("Seckill order message returned but messageId header is missing, exchange={}, routingKey={}, replyCode={}, replyText={}, body={}",
                    exchange,
                    routingKey,
                    replyCode,
                    replyText,
                    message);
            return;
        }
        String reason = limitReason("RETURNED replyCode=" + replyCode
                + ", replyText=" + replyText
                + ", exchange=" + exchange
                + ", routingKey=" + routingKey);
        LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(returnedNextRetryDelaySeconds);
        boolean updated = mqMessageService.markReturned(messageId, reason, nextRetryTime);
        log.error("Seckill order message returned, messageId={}, updated={}, exchange={}, routingKey={}, replyCode={}, replyText={}, body={}",
                messageId,
                updated,
                exchange,
                routingKey,
                replyCode,
                replyText,
                message);
    }

    private Long parseMessageId(Object rawMessageId, String callbackType) {
        if (rawMessageId == null) {
            log.error("Seckill order mq {} callback missing messageId", callbackType);
            return null;
        }
        try {
            return Long.valueOf(rawMessageId.toString());
        } catch (NumberFormatException e) {
            log.error("Seckill order mq {} callback has invalid messageId={}", callbackType, rawMessageId, e);
            return null;
        }
    }

    private boolean isInternalCorrelation(CorrelationData correlationData) {
        return correlationData != null
                && correlationData.getId() != null
                && (correlationData.getId().startsWith("outbox:")
                || correlationData.getId().startsWith(VoucherOrderConsumer.CLAIM_TRANSFER_CORRELATION_PREFIX));
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 512 ? reason : reason.substring(0, 512);
    }
}
