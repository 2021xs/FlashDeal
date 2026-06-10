package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.dto.OrderProcessStatus;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_QUEUE;

@Slf4j
@Component
public class VoucherOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private IMqMessageService mqMessageService;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private SeckillReservationService seckillReservationService;

    @Value("${seckill.order.batch-consume.retry-times:2}")
    private int retryTimes;

    @RabbitListener(
            queues = SECKILL_ORDER_QUEUE,
            containerFactory = "seckillOrderBatchRabbitListenerContainerFactory")
    public void handleSeckillOrderBatch(List<Message> messages, Channel channel) throws Exception {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        List<BatchMessageItem> items = new ArrayList<>(messages.size());
        for (Message message : messages) {
            try {
                VoucherOrderMessage orderMessage = objectMapper.readValue(message.getBody(), VoucherOrderMessage.class);
                items.add(new BatchMessageItem(message, orderMessage));
            } catch (Exception e) {
                long deliveryTag = message.getMessageProperties().getDeliveryTag();
                log.error("Parse seckill order batch message failed, deliveryTag={}", deliveryTag, e);
                channel.basicNack(deliveryTag, false, false);
            }
        }
        processWithRetry(prepareClaimedItems(items, channel), channel);
    }

    private List<BatchMessageItem> prepareClaimedItems(List<BatchMessageItem> items, Channel channel) throws IOException {
        List<BatchMessageItem> claimedItems = new ArrayList<>();
        for (BatchMessageItem item : items) {
            if (voucherOrderService.hasExistingOrder(item.getOrderId(), item.getUserId(), item.getVoucherId())) {
                if (markConsumed(item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId())) {
                    cleanupPendingSafely(item);
                    channel.basicAck(item.getDeliveryTag(), false);
                } else {
                    channel.basicNack(item.getDeliveryTag(), false, false);
                }
                continue;
            }
            if (seckillReservationService.claim(item.getVoucherId(), item.getUserId(), item.getOrderId())) {
                claimedItems.add(item);
                continue;
            }
            log.warn("Skip stale or duplicate seckill message because reservation claim failed, messageId={}, orderId={}, userId={}, voucherId={}",
                    item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId());
            channel.basicAck(item.getDeliveryTag(), false);
        }
        return claimedItems;
    }

    private void processWithRetry(List<BatchMessageItem> items, Channel channel) throws IOException {
        List<BatchMessageItem> retryItems = new ArrayList<>(items);
        int maxAttempts = Math.max(1, retryTimes + 1);
        for (int attempt = 1; attempt <= maxAttempts && !retryItems.isEmpty(); attempt++) {
            BatchVoucherOrderResult result = createBatchResult(retryItems);
            List<BatchMessageItem> nextRetryItems = handleBatchResult(retryItems, result, channel, attempt, maxAttempts);
            retryItems = nextRetryItems;
        }
    }

    private BatchVoucherOrderResult createBatchResult(List<BatchMessageItem> items) {
        List<VoucherOrderMessage> messages = items.stream()
                .map(BatchMessageItem::getOrderMessage)
                .collect(Collectors.toList());
        try {
            return voucherOrderService.createClaimedVoucherOrdersBatch(messages);
        } catch (Exception e) {
            BatchVoucherOrderResult result = new BatchVoucherOrderResult();
            String reason = summarizeException(e);
            for (BatchMessageItem item : items) {
                result.addRetryableFailedOrderId(item.getOrderId(), reason);
            }
            result.setBatchLevelException(reason);
            log.error("Seckill order batch listener service call failed, size={}", items.size(), e);
            return result;
        }
    }

    private List<BatchMessageItem> handleBatchResult(List<BatchMessageItem> items,
                                                     BatchVoucherOrderResult result,
                                                     Channel channel,
                                                     int attempt,
                                                     int maxAttempts) throws IOException {
        List<BatchMessageItem> nextRetryItems = new ArrayList<>();
        Map<Long, OrderProcessStatus> statusMap = result.getOrderStatusMap();
        for (BatchMessageItem item : items) {
            Long orderId = item.getOrderId();
            OrderProcessStatus status = statusMap.get(orderId);
            if (status == OrderProcessStatus.SUCCESS || status == OrderProcessStatus.IDEMPOTENT_SUCCESS) {
                cleanupPendingSafely(item);
                channel.basicAck(item.getDeliveryTag(), false);
            } else if (status == OrderProcessStatus.NON_RETRYABLE_FAILED) {
                log.warn("Seckill order batch message non-retryable failure, messageId={}, orderId={}, reason={}",
                        item.getMessageId(), orderId, result.getFailedReasons().get(orderId));
                channel.basicNack(item.getDeliveryTag(), false, false);
            } else if (status == OrderProcessStatus.RETRYABLE_FAILED || status == null) {
                if (attempt < maxAttempts) {
                    nextRetryItems.add(item);
                } else {
                    log.error("Seckill order batch message retry exhausted, messageId={}, orderId={}, reason={}, attempt={}/{}",
                            item.getMessageId(), orderId, result.getFailedReasons().get(orderId), attempt, maxAttempts);
                    channel.basicNack(item.getDeliveryTag(), false, false);
                }
            }
        }
        return nextRetryItems;
    }

    private boolean markConsumed(Long messageId, Long orderId, Long userId, Long voucherId) {
        boolean updated;
        try {
            if (messageId != null) {
                updated = mqMessageService.markConsumed(messageId);
            } else {
                updated = mqMessageService.markConsumedByBiz(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, orderId);
            }
        } catch (Exception e) {
            log.error("Mark seckill mq message CONSUMED threw exception after order success, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId, e);
            return false;
        }
        if (!updated) {
            log.error("Mark seckill mq message CONSUMED failed after order success, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId);
        }
        return updated;
    }

    private void cleanupPendingSafely(BatchMessageItem item) {
        try {
            seckillReservationService.cleanupPending(item.getOrderId());
        } catch (Exception e) {
            log.error("Clean seckill pending failed after MySQL order confirmed, messageId={}, orderId={}",
                    item.getMessageId(), item.getOrderId(), e);
        }
    }

    private String summarizeException(Throwable e) {
        if (e == null) {
            return null;
        }
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static class BatchMessageItem {
        private final Message message;
        private final VoucherOrderMessage orderMessage;

        private BatchMessageItem(Message message, VoucherOrderMessage orderMessage) {
            this.message = message;
            this.orderMessage = orderMessage;
        }

        private VoucherOrderMessage getOrderMessage() {
            return orderMessage;
        }

        private Long getMessageId() {
            return orderMessage.getMessageId();
        }

        private Long getOrderId() {
            return orderMessage.getOrderId();
        }

        private Long getUserId() {
            return orderMessage.getUserId();
        }

        private Long getVoucherId() {
            return orderMessage.getVoucherId();
        }

        private long getDeliveryTag() {
            return message.getMessageProperties().getDeliveryTag();
        }
    }
}
