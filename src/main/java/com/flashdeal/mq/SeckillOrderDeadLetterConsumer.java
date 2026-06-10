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

            Long rollback = seckillReservationService.rollback(voucherId, userId, orderId);
            if (rollback != null && (rollback == 0L || rollback == 1L)) {
                markFailedAfterDlqRollback(messageId, orderId, userId, voucherId,
                        "DLQ_ROLLBACK_DONE_ORDER_NOT_FOUND result=" + rollback);
                log.warn("Seckill order DLQ message rolled back Redis state, messageId={}, orderId={}, userId={}, voucherId={}, rollback={}",
                        messageId, orderId, userId, voucherId, rollback);
            } else {
                markNeedManual(messageId, orderId, userId, voucherId, "DLQ_ROLLBACK_SKIPPED");
                log.error("Seckill order DLQ reservation rollback skipped, messageId={}, orderId={}, userId={}, voucherId={}, rollback={}",
                        messageId, orderId, userId, voucherId, rollback);
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

    private void markFailedAfterDlqRollback(Long messageId, Long orderId, Long userId, Long voucherId, String reason) {
        boolean updated = messageId != null
                ? mqMessageService.markFailedAfterDlqRollback(messageId, reason)
                : mqMessageService.markFailedAfterDlqRollbackByBiz(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, orderId, reason);
        if (!updated) {
            log.error("Mark DLQ seckill mq message FAILED after rollback failed, messageId={}, orderId={}, userId={}, voucherId={}, reason={}",
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
