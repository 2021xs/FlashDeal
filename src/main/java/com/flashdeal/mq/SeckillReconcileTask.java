package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import com.flashdeal.service.IOutboxEventService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

import static com.flashdeal.service.IMqMessageService.SECKILL_ORDER_BIZ_TYPE;
import static com.flashdeal.service.IOutboxEventService.ORDER_TIMEOUT_EVENT;
import static com.flashdeal.service.IOutboxEventService.REDIS_STOCK_RECOVERY_EVENT;

@Slf4j
@Component
public class SeckillReconcileTask {

    @Resource
    private IMqMessageService mqMessageService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private SeckillReservationService seckillReservationService;

    @Resource
    private IOutboxEventService outboxEventService;

    @Resource
    private IOrderTimeoutCloseFailService orderTimeoutCloseFailService;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${seckill.reconcile.enabled:true}")
    private Boolean reconcileEnabled;

    @Value("${seckill.reconcile.batch-size:100}")
    private Integer reconcileBatchSize;

    @Value("${seckill.reconcile.alert-suppress-seconds:1800}")
    private Long alertSuppressSeconds;

    @Scheduled(fixedDelayString = "#{${seckill.reconcile.fixed-delay-seconds:60} * 1000}")
    public void reconcileNeedManualSeckillMessages() {
        if (!Boolean.TRUE.equals(reconcileEnabled)) {
            return;
        }

        // Only NEED_MANUAL is scanned here. Retryable MQ states are owned by MqMessageRetryTask;
        // rolling back Redis while MQ may still be resent would break the seckill state.
        List<MqMessage> messages = mqMessageService.listNeedManualMessages(
                SECKILL_ORDER_BIZ_TYPE,
                safeBatchSize());
        for (MqMessage message : messages) {
            reconcileOne(message);
        }
        inspectOrderTimeoutOutboxNeedManual();
        inspectRedisStockRecoveryOutboxNeedManual();
        inspectOrderTimeoutCloseFailNeedManual();
    }

    private void reconcileOne(MqMessage message) {
        Long messageId = message.getId();
        String oldStatus = message.getStatus();
        VoucherOrderMessage orderMessage;
        try {
            orderMessage = objectMapper.readValue(message.getMessageBody(), VoucherOrderMessage.class);
        } catch (Exception e) {
            keepNeedManual(message, null, null, null, "RECONCILE_MESSAGE_BODY_PARSE_FAILED", e);
            return;
        }

        Long orderId = orderMessage.getOrderId() == null ? message.getBizId() : orderMessage.getOrderId();
        Long userId = orderMessage.getUserId();
        Long voucherId = orderMessage.getVoucherId();
        if (orderId == null || userId == null || voucherId == null) {
            keepNeedManual(message, orderId, userId, voucherId, "RECONCILE_MESSAGE_FIELD_MISSING", null);
            return;
        }

        // MySQL is the final fact source: if the order exists, Redis must not be rolled back.
        VoucherOrder existingOrder = findExistingOrder(orderId, userId, voucherId);
        if (existingOrder != null) {
            boolean updated = mqMessageService.markConsumedAfterReconcile(messageId);
            log.info("Seckill reconcile fixed message as consumed, messageId={}, orderId={}, voucherId={}, userId={}, oldStatus={}, reconcileResult=ORDER_EXISTS, updated={}",
                    messageId, orderId, voucherId, userId, oldStatus, updated);
            return;
        }

        if (seckillReservationService.hasPending(orderId)) {
            boolean updated = mqMessageService.markFailedAfterReconcile(
                    messageId, "WAITING_REDIS_MYSQL_RECONCILE");
            log.info("Seckill reconcile removed message from NEED_MANUAL because Redis pending still exists; Redis-MySQL reconcile owns rollback, messageId={}, orderId={}, voucherId={}, userId={}, oldStatus={}, updated={}",
                    messageId, orderId, voucherId, userId, oldStatus, updated);
            return;
        }

        keepNeedManual(message, orderId, userId, voucherId,
                "ORDER_NOT_FOUND_AND_REDIS_PENDING_ABSENT", null);
    }

    private void inspectOrderTimeoutOutboxNeedManual() {
        inspectOutboxNeedManual(ORDER_TIMEOUT_EVENT, "order_timeout_outbox");
    }

    private void inspectRedisStockRecoveryOutboxNeedManual() {
        inspectOutboxNeedManual(REDIS_STOCK_RECOVERY_EVENT, "redis_stock_recovery_outbox");
    }

    private void inspectOutboxNeedManual(String eventType, String source) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime suppressBefore = now.minusSeconds(safeAlertSuppressSeconds());
        List<OutboxEvent> events = outboxEventService.listNeedManualForAlert(eventType, suppressBefore, safeBatchSize());
        for (OutboxEvent event : events) {
            boolean alerted = outboxEventService.markNeedManualAlerted(event.getId(), now, suppressBefore);
            if (alerted) {
                log.error("Need manual inspection alert, source={}, table=tb_outbox_event, eventId={}, eventType={}, bizKey={}, retryCount={}, maxRetryCount={}, failReason={}",
                        source, event.getEventId(), event.getEventType(), event.getBizKey(),
                        event.getRetryCount(), event.getMaxRetryCount(), event.getFailReason());
            }
        }
    }

    private void inspectOrderTimeoutCloseFailNeedManual() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime suppressBefore = now.minusSeconds(safeAlertSuppressSeconds());
        List<OrderTimeoutCloseFail> failures =
                orderTimeoutCloseFailService.listNeedManualForAlert(suppressBefore, safeBatchSize());
        for (OrderTimeoutCloseFail failure : failures) {
            boolean alerted = orderTimeoutCloseFailService.markNeedManualAlerted(
                    failure.getId(), now, suppressBefore);
            if (alerted) {
                log.error("Need manual inspection alert, source=order_timeout_close_fail, table=tb_order_timeout_close_fail, orderId={}, userId={}, voucherId={}, failCount={}, maxFailCount={}, failReason={}",
                        failure.getOrderId(), failure.getUserId(), failure.getVoucherId(),
                        failure.getFailCount(), failure.getMaxFailCount(), failure.getLastFailReason());
            }
        }
    }

    private VoucherOrder findExistingOrder(Long orderId, Long userId, Long voucherId) {
        VoucherOrder order = voucherOrderService.getById(orderId);
        if (order != null) {
            return order;
        }
        return voucherOrderService.query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .one();
    }

    private void keepNeedManual(MqMessage message,
                                Long orderId,
                                Long userId,
                                Long voucherId,
                                String reason,
                                Exception e) {
        boolean updated = mqMessageService.markNeedManualAfterReconcile(message.getId(), limitReason(reason));
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime suppressBefore = now.minusSeconds(safeAlertSuppressSeconds());
        boolean alerted = mqMessageService.markNeedManualAlerted(message.getId(), now, suppressBefore);
        if (alerted) {
            log.error("Need manual inspection alert, source=seckill_mq_message, table=tb_mq_message, messageId={}, orderId={}, voucherId={}, userId={}, oldStatus={}, reconcileResult=NEED_MANUAL, failReason={}, updated={}",
                    message.getId(), orderId, voucherId, userId, message.getStatus(), reason, updated, e);
        } else {
            log.debug("Seckill reconcile keeps message NEED_MANUAL without duplicate alert, messageId={}, orderId={}, voucherId={}, userId={}, failReason={}, updated={}",
                    message.getId(), orderId, voucherId, userId, reason, updated, e);
        }
    }

    private int safeBatchSize() {
        return reconcileBatchSize == null || reconcileBatchSize <= 0 ? 100 : reconcileBatchSize;
    }

    private long safeAlertSuppressSeconds() {
        return alertSuppressSeconds == null || alertSuppressSeconds <= 0 ? 1800L : alertSuppressSeconds;
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 512 ? reason : reason.substring(0, 512);
    }
}
