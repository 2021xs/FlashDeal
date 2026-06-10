package com.flashdeal.mq;

import com.flashdeal.dto.SeckillPendingDetail;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillRedisMysqlReconcileTaskTest {

    @Test
    void missingDetailShouldOnlyRemovePendingMember() {
        Fixture fixture = new Fixture();
        fixture.run();
        verify(fixture.reservationService).removePendingMember(10L);
        verify(fixture.reservationService, never()).rollback(30L, 20L, 10L, fixture.now);
    }

    @Test
    void existingMysqlOrderShouldOnlyCleanupPending() {
        Fixture fixture = new Fixture();
        fixture.givenDetail();
        when(fixture.orderService.hasExistingOrder(10L, 20L, 30L)).thenReturn(true);
        fixture.run();
        verify(fixture.reservationService).cleanupPending(10L);
        verify(fixture.reservationService, never()).rollback(30L, 20L, 10L, fixture.now);
    }

    @Test
    void pendingWithoutMysqlOrderShouldRollback() {
        Fixture fixture = new Fixture();
        fixture.givenDetail();
        when(fixture.reservationService.getReservation(30L, 20L)).thenReturn("10:PENDING:1");
        fixture.run();
        verify(fixture.reservationService).rollback(30L, 20L, 10L, fixture.now);
    }

    @Test
    void processingWithinTimeoutShouldBeSkipped() {
        Fixture fixture = new Fixture();
        fixture.givenDetail();
        when(fixture.reservationService.getReservation(30L, 20L))
                .thenReturn("10:PROCESSING:" + (fixture.now - 1000));
        when(fixture.reservationService.getProcessingTimeoutMillis()).thenReturn(600_000L);
        fixture.run();
        verify(fixture.reservationService, never()).rollback(30L, 20L, 10L, fixture.now);
    }

    @Test
    void processingTimeoutShouldRecheckMysqlThenRollback() {
        Fixture fixture = new Fixture();
        fixture.givenDetail();
        when(fixture.reservationService.getReservation(30L, 20L)).thenReturn("10:PROCESSING:1");
        when(fixture.reservationService.getProcessingTimeoutMillis()).thenReturn(600_000L);
        fixture.run();
        verify(fixture.orderService, org.mockito.Mockito.times(2)).hasExistingOrder(10L, 20L, 30L);
        verify(fixture.reservationService).rollback(30L, 20L, 10L, fixture.now);
    }

    private static class Fixture {
        private final SeckillRedisMysqlReconcileTask task = new SeckillRedisMysqlReconcileTask();
        private final SeckillReservationService reservationService = mock(SeckillReservationService.class);
        private final IVoucherOrderService orderService = mock(IVoucherOrderService.class);
        private final long now = System.currentTimeMillis();

        private Fixture() {
            ReflectionTestUtils.setField(task, "seckillReservationService", reservationService);
            ReflectionTestUtils.setField(task, "voucherOrderService", orderService);
            ReflectionTestUtils.setField(task, "enabled", true);
            ReflectionTestUtils.setField(task, "batchSize", 100);
            ReflectionTestUtils.setField(task, "pendingTimeoutSeconds", 300L);
            when(reservationService.listExpiredPending(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyInt()))
                    .thenReturn(Collections.singleton("10"));
        }

        private void givenDetail() {
            SeckillPendingDetail detail = new SeckillPendingDetail();
            detail.setOrderId(10L);
            detail.setUserId(20L);
            detail.setVoucherId(30L);
            when(reservationService.getPendingDetail(10L)).thenReturn(detail);
        }

        private void run() {
            ReflectionTestUtils.invokeMethod(task, "reconcileOne", "10", now);
        }
    }
}
