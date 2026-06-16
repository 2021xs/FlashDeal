package com.flashdeal.mq;

import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLQ;

@Slf4j
@Component
public class SeckillOrderDeadLetterConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private SeckillReservationService seckillReservationService;

    @Resource
    private IMqMessageService mqMessageService;

    @RabbitListener(queues = SECKILL_ORDER_DLQ, containerFactory = "rabbitListenerContainerFactory")
    public void handleDeadLetter(VoucherOrderMessage orderMessage, Message message, Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Long orderId = orderMessage.getOrderId();
        Long userId = orderMessage.getUserId();
        Long voucherId = orderMessage.getVoucherId();
        Long messageId = orderMessage.getMessageId();
        try {
            if (voucherOrderService.hasExistingOrder(orderId, userId, voucherId)) {
                markConsumed(messageId, orderId, userId, voucherId);
                seckillReservationService.cleanupPending(orderId);
                log.warn("Seckill order DLQ message ignored because order already exists, messageId={}, orderId={}, userId={}, voucherId={}",
                        messageId, orderId, userId, voucherId);
                channel.basicAck(deliveryTag, false);
                return;
            }

            if (seckillReservationService.hasPending(orderId)) {
                markFailedAfterDlqInspection(messageId, orderId, userId, voucherId,
                        "DLQ_ORDER_NOT_FOUND_WAIT_REDIS_RECONCILE");
                log.warn("Seckill order DLQ message marked failed and waits Redis-MySQL reconcile, messageId={}, orderId={}, userId={}, voucherId={}",
                        messageId, orderId, userId, voucherId);
            } else {
                markNeedManual(messageId, orderId, userId, voucherId, "DLQ_ORDER_AND_REDIS_PENDING_ABSENT");
                log.warn("Seckill order DLQ message marked NEED_MANUAL because order and Redis pending are absent, messageId={}, orderId={}, userId={}, voucherId={}",
                        messageId, orderId, userId, voucherId);
            }
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            markNeedManual(messageId, orderId, userId, voucherId, "DLQ_COMPENSATION_EXCEPTION");
            log.error("Seckill order DLQ compensation failed, ack to avoid endless DLQ loop, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId, e);
            channel.basicAck(deliveryTag, false);
        }
    }

    private void markConsumed(Long messageId, Long orderId, Long userId, Long voucherId) {
        boolean updated = messageId != null
                ? mqMessageService.markConsumed(messageId)
                : mqMessageService.markConsumedByBiz(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, orderId);
        if (!updated) {
            log.error("Mark DLQ seckill mq message CONSUMED failed, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId);
        }
    }

    private void markFailedAfterDlqInspection(Long messageId, Long orderId, Long userId, Long voucherId, String reason) {
        boolean updated = messageId != null
                ? mqMessageService.markFailedAfterDlqInspection(messageId, reason)
                : mqMessageService.markFailedAfterDlqInspectionByBiz(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, orderId, reason);
        if (!updated) {
            log.error("Mark DLQ seckill mq message FAILED after inspection failed, messageId={}, orderId={}, userId={}, voucherId={}, reason={}",
                    messageId, orderId, userId, voucherId, reason);
        }
    }

    private void markNeedManual(Long messageId, Long orderId, Long userId, Long voucherId, String reason) {
        boolean updated = false;
        if (messageId != null) {
            updated = mqMessageService.markNeedManual(messageId, reason);
        }
        if (!updated) {
            log.error("Mark DLQ seckill mq message NEED_MANUAL failed, messageId={}, orderId={}, userId={}, voucherId={}, reason={}",
                    messageId, orderId, userId, voucherId, reason);
        }
    }
}
