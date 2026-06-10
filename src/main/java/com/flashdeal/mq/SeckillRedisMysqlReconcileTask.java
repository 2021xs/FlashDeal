package com.flashdeal.mq;

import com.flashdeal.dto.SeckillPendingDetail;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;

@Slf4j
@Component
public class SeckillRedisMysqlReconcileTask {

    @Resource
    private SeckillReservationService seckillReservationService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Value("${seckill.redis-reconcile.enabled:true}")
    private boolean enabled;

    @Value("${seckill.redis-reconcile.batch-size:100}")
    private int batchSize;

    @Value("${seckill.redis-reconcile.pending-timeout-seconds:300}")
    private long pendingTimeoutSeconds;

    @Scheduled(fixedDelayString = "#{${seckill.redis-reconcile.fixed-delay-seconds:60} * 1000}")
    public void reconcileExpiredPending() {
        if (!enabled) {
            return;
        }
        long nowMillis = System.currentTimeMillis();
        long maxScore = nowMillis - Math.max(0L, pendingTimeoutSeconds) * 1000L;
        Set<String> orderIds = seckillReservationService.listExpiredPending(maxScore, batchSize);
        if (orderIds == null || orderIds.isEmpty()) {
            return;
        }
        for (String rawOrderId : orderIds) {
            reconcileOne(rawOrderId, nowMillis);
        }
    }

    private void reconcileOne(String rawOrderId, long nowMillis) {
        Long orderId = parseOrderId(rawOrderId);
        if (orderId == null) {
            return;
        }
        SeckillPendingDetail detail = seckillReservationService.getPendingDetail(orderId);
        if (!valid(detail)) {
            seckillReservationService.removePendingMember(orderId);
            log.warn("Remove seckill pending member because detail is missing or invalid, orderId={}", orderId);
            return;
        }

        if (voucherOrderService.hasExistingOrder(orderId, detail.getUserId(), detail.getVoucherId())) {
            seckillReservationService.cleanupPending(orderId);
            log.info("Clean seckill pending because MySQL order exists, orderId={}, userId={}, voucherId={}",
                    orderId, detail.getUserId(), detail.getVoucherId());
            return;
        }

        String reservation = seckillReservationService.getReservation(detail.getVoucherId(), detail.getUserId());
        String pendingPrefix = orderId + ":PENDING:";
        String processingPrefix = orderId + ":PROCESSING:";
        if (reservation != null && reservation.startsWith(pendingPrefix)) {
            rollback(detail, nowMillis, "PENDING_TIMEOUT");
            return;
        }
        if (reservation != null && reservation.startsWith(processingPrefix)) {
            Long processingTime = parseTimestamp(reservation.substring(processingPrefix.length()));
            if (processingTime != null
                    && nowMillis - processingTime < seckillReservationService.getProcessingTimeoutMillis()) {
                return;
            }
            // Recheck after the timeout decision to narrow the race with a committing consumer.
            if (voucherOrderService.hasExistingOrder(orderId, detail.getUserId(), detail.getVoucherId())) {
                seckillReservationService.cleanupPending(orderId);
                return;
            }
            rollback(detail, nowMillis, "PROCESSING_TIMEOUT");
            return;
        }

        seckillReservationService.cleanupPending(orderId);
        log.warn("Clean stale seckill pending because reservation no longer matches, orderId={}, reservation={}",
                orderId, reservation);
    }

    private void rollback(SeckillPendingDetail detail, long nowMillis, String reason) {
        Long result = seckillReservationService.rollback(
                detail.getVoucherId(), detail.getUserId(), detail.getOrderId(), nowMillis);
        log.warn("Reconcile seckill pending rollback, reason={}, result={}, orderId={}, userId={}, voucherId={}",
                reason, result, detail.getOrderId(), detail.getUserId(), detail.getVoucherId());
    }

    private boolean valid(SeckillPendingDetail detail) {
        return detail != null
                && detail.getOrderId() != null
                && detail.getUserId() != null
                && detail.getVoucherId() != null;
    }

    private Long parseOrderId(String value) {
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            log.error("Invalid orderId in seckill pending zset, value={}", value);
            return null;
        }
    }

    private Long parseTimestamp(String value) {
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
