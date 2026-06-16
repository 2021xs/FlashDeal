package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.dto.OrderProcessStatus;
import com.flashdeal.dto.SeckillReservationState;
import com.flashdeal.dto.SeckillReservationStatus;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.flashdeal.utils.RabbitConstants.SECKILL_CLAIM_RETRY_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_CLAIM_RETRY_ROUTING_KEY;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_QUEUE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLK;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLX;

@Slf4j
@Component
public class VoucherOrderConsumer {

    private static final String CLAIM_RETRY_COUNT_HEADER = "x-seckill-claim-retry-count";
    private static final String CLAIM_LAST_STATUS_HEADER = "x-seckill-claim-last-status";
    private static final String CLAIM_LAST_REASON_HEADER = "x-seckill-claim-last-reason";
    private static final String CLAIM_LAST_TIME_HEADER = "x-seckill-claim-last-time";
    static final String CLAIM_TRANSFER_HEADER = "x-seckill-claim-transfer";
    static final String CLAIM_TRANSFER_CORRELATION_PREFIX = "claim-transfer:";

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private IMqMessageService mqMessageService;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private SeckillReservationService seckillReservationService;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Value("${seckill.order.batch-consume.retry-times:2}")
    private int retryTimes;

    @Value("${seckill.reservation.claim-retry.enabled:true}")
    private boolean claimRetryEnabled;

    @Value("${seckill.reservation.claim-retry.max-attempts:5}")
    private int claimRetryMaxAttempts;

    @Value("${seckill.reservation.claim-transfer-confirm-timeout-ms:5000}")
    private long claimTransferConfirmTimeoutMillis;

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
            if (seckillReservationService.claim(item.getVoucherId(), item.getUserId(), item.getOrderId())) {
                claimedItems.add(item);
                continue;
            }
            if (handleClaimFailure(item, channel)) {
                claimedItems.add(item);
            }
        }
        return claimedItems;
    }

    private boolean handleClaimFailure(BatchMessageItem item, Channel channel) throws IOException {
        SeckillReservationState state = seckillReservationService.getReservationState(
                item.getVoucherId(), item.getUserId());
        SeckillReservationStatus status = state.getStatus();
        Long stateOrderId = state.getOrderId();
        if (stateOrderId != null && !stateOrderId.equals(item.getOrderId())) {
            log.warn("Nack seckill message because reservation belongs to another order, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}, reservationOrderId={}, reservation={}",
                    item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                    status, stateOrderId, state.getRawValue());
            sendToClaimFailureOutletOrFallback(item, channel, state, "RESERVATION_BELONGS_TO_ANOTHER_ORDER");
            return false;
        }

        if (status == SeckillReservationStatus.COMMITTED
                || status == SeckillReservationStatus.CANCELED
                || status == SeckillReservationStatus.EXPIRED) {
            if (markConsumed(item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId())) {
                cleanupPendingSafely(item);
                log.info("Ack seckill message after reservation terminal state, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}",
                        item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(), status);
                channel.basicAck(item.getDeliveryTag(), false);
            } else {
                channel.basicNack(item.getDeliveryTag(), false, true);
            }
            return false;
        }

        String reason = buildClaimFailureRequeueReason(status);
        retryClaimLaterOrSendToFailureOutlet(item, channel, state, reason);
        return false;
    }

    private void retryClaimLaterOrSendToFailureOutlet(BatchMessageItem item,
                                                      Channel channel,
                                                      SeckillReservationState state,
                                                      String reason) throws IOException {
        int retryCount = getClaimRetryCount(item.getMessage());
        int maxAttempts = Math.max(0, claimRetryMaxAttempts);
        if (claimRetryEnabled && retryCount < maxAttempts) {
            retryClaimLater(item, channel, state, reason, retryCount, maxAttempts);
            return;
        }
        sendToClaimFailureOutletOrFallback(item, channel, state, reason);
    }

    private void retryClaimLater(BatchMessageItem item,
                                 Channel channel,
                                 SeckillReservationState state,
                                 String reason,
                                 int retryCount,
                                 int maxAttempts) throws IOException {
        int nextRetryCount = retryCount + 1;
        Message transferMessage = buildClaimTransferMessage(item, state, reason, nextRetryCount);
        try {
            publishClaimTransferAndWaitConfirm(
                    SECKILL_CLAIM_RETRY_EXCHANGE,
                    SECKILL_CLAIM_RETRY_ROUTING_KEY,
                    transferMessage,
                    "retry",
                    item);
            log.warn("Send seckill message to claim retry queue, reason={}, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}, retryCount={}, maxAttempts={}, action=retry",
                    reason, item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                    state.getStatus(), nextRetryCount, maxAttempts);
            channel.basicAck(item.getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("Send seckill claim retry message failed, fallback requeue, reason={}, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}, retryCount={}, maxAttempts={}, action=fallback_requeue",
                    reason, item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                    state.getStatus(), retryCount, maxAttempts, e);
            channel.basicNack(item.getDeliveryTag(), false, true);
        }
    }

    private void sendToClaimFailureOutletOrFallback(BatchMessageItem item,
                                                    Channel channel,
                                                    SeckillReservationState state,
                                                    String reason) throws IOException {
        int retryCount = getClaimRetryCount(item.getMessage());
        int maxAttempts = Math.max(0, claimRetryMaxAttempts);
        Message transferMessage = buildClaimTransferMessage(item, state, reason, retryCount);
        try {
            publishClaimTransferAndWaitConfirm(
                    SECKILL_ORDER_DLX,
                    SECKILL_ORDER_DLK,
                    transferMessage,
                    "dlq",
                    item);
            log.error("Send seckill message to claim failure outlet, reason={}, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}, retryCount={}, maxAttempts={}, action=send_to_dlq",
                    reason, item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                    state.getStatus(), retryCount, maxAttempts);
            channel.basicAck(item.getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("Send seckill claim failure outlet message failed, fallback requeue, reason={}, messageId={}, orderId={}, userId={}, voucherId={}, reservationStatus={}, retryCount={}, maxAttempts={}, action=fallback_requeue",
                    reason, item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                    state.getStatus(), retryCount, maxAttempts, e);
            channel.basicNack(item.getDeliveryTag(), false, true);
        }
    }

    private Message buildClaimTransferMessage(BatchMessageItem item,
                                              SeckillReservationState state,
                                              String reason,
                                              int retryCount) {
        return MessageBuilder.withBody(item.getMessage().getBody())
                .copyProperties(item.getMessage().getMessageProperties())
                .setHeader(CLAIM_TRANSFER_HEADER, true)
                .setHeader(CLAIM_RETRY_COUNT_HEADER, retryCount)
                .setHeader(CLAIM_LAST_STATUS_HEADER, state.getStatus() == null ? null : state.getStatus().name())
                .setHeader(CLAIM_LAST_REASON_HEADER, reason)
                .setHeader(CLAIM_LAST_TIME_HEADER, System.currentTimeMillis())
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
    }

    private void publishClaimTransferAndWaitConfirm(String exchange,
                                                    String routingKey,
                                                    Message message,
                                                    String action,
                                                    BatchMessageItem item) throws Exception {
        CorrelationData correlationData = new CorrelationData(CLAIM_TRANSFER_CORRELATION_PREFIX
                + action + ":" + item.getMessageId() + ":" + System.nanoTime());
        rabbitTemplate.convertAndSend(exchange, routingKey, message, correlationData);
        CorrelationData.Confirm confirm = correlationData.getFuture().get(
                Math.max(1L, claimTransferConfirmTimeoutMillis), TimeUnit.MILLISECONDS);
        if (!confirm.isAck()) {
            throw new IllegalStateException("claim transfer publisher confirm nack, reason=" + confirm.getReason());
        }
        if (correlationData.getReturnedMessage() != null) {
            throw new IllegalStateException("claim transfer message returned by broker");
        }
    }

    private int getClaimRetryCount(Message message) {
        Object value = message.getMessageProperties().getHeaders().get(CLAIM_RETRY_COUNT_HEADER);
        if (value instanceof Number) {
            return Math.max(0, ((Number) value).intValue());
        }
        if (value != null) {
            try {
                return Math.max(0, Integer.parseInt(value.toString()));
            } catch (NumberFormatException e) {
                log.warn("Invalid seckill claim retry header, value={}", value);
            }
        }
        return 0;
    }

    private String buildClaimFailureRequeueReason(SeckillReservationStatus status) {
        if (status == SeckillReservationStatus.PROCESSING) {
            return "PROCESSING_WAIT_RECONCILE";
        }
        if (status == SeckillReservationStatus.MISSING) {
            return "MISSING_RESERVATION";
        }
        if (status == SeckillReservationStatus.UNKNOWN) {
            return "UNKNOWN_RESERVATION_STATUS";
        }
        if (status == SeckillReservationStatus.PENDING) {
            return "PENDING_CLAIM_RACE";
        }
        return "NON_TERMINAL_RESERVATION";
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
                commitReservationSafely(item);
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

    private void commitReservationSafely(BatchMessageItem item) {
        try {
            Long result = seckillReservationService.commit(item.getVoucherId(), item.getUserId(), item.getOrderId());
            if (result != null && result == 1L) {
                log.debug("Commit seckill reservation after MySQL order confirmed, messageId={}, orderId={}, userId={}, voucherId={}, action=commitReservation",
                        item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId());
            } else {
                log.warn("Commit seckill reservation skipped because reservation state did not match confirmed order, messageId={}, orderId={}, userId={}, voucherId={}, reservationKey=seckill:reservation:{}:{}, commitResult={}, action=ackOrderButExposeReservationMismatch",
                        item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(),
                        item.getVoucherId(), item.getUserId(), result);
            }
        } catch (Exception e) {
            log.error("Commit seckill reservation failed after MySQL order confirmed, messageId={}, orderId={}, userId={}, voucherId={}",
                    item.getMessageId(), item.getOrderId(), item.getUserId(), item.getVoucherId(), e);
            cleanupPendingSafely(item);
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

        private Message getMessage() {
            return message;
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
