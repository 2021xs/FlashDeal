package com.flashdeal.mq;

import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import com.flashdeal.service.IVoucherOrderService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderTimeoutCloseRetryTaskTest {

    @Test
    void successfulRetryShouldMarkFailureHandled() {
        Fixture fixture = new Fixture();
        OrderTimeoutCloseFail failure = fixture.failure();
        when(fixture.failService.claimRetry(failure)).thenReturn(true);

        fixture.task.retryOne(failure);

        verify(fixture.orderService).closeTimeoutOrder(10L);
        verify(fixture.failService).markHandled(failure);
    }

    @Test
    void failedRetryShouldScheduleBackoff() {
        Fixture fixture = new Fixture();
        OrderTimeoutCloseFail failure = fixture.failure();
        when(fixture.failService.claimRetry(failure)).thenReturn(true);
        doThrow(new IllegalStateException("mysql down")).when(fixture.orderService).closeTimeoutOrder(10L);

        fixture.task.retryOne(failure);

        verify(fixture.failService).markRetryFailed(
                org.mockito.ArgumentMatchers.eq(failure),
                org.mockito.ArgumentMatchers.contains("mysql down"),
                org.mockito.ArgumentMatchers.eq(60L),
                org.mockito.ArgumentMatchers.eq(3600L));
    }

    private static class Fixture {
        private final OrderTimeoutCloseRetryTask task = new OrderTimeoutCloseRetryTask();
        private final IOrderTimeoutCloseFailService failService = mock(IOrderTimeoutCloseFailService.class);
        private final IVoucherOrderService orderService = mock(IVoucherOrderService.class);

        private Fixture() {
            ReflectionTestUtils.setField(task, "failService", failService);
            ReflectionTestUtils.setField(task, "voucherOrderService", orderService);
            ReflectionTestUtils.setField(task, "baseRetryDelaySeconds", 60L);
            ReflectionTestUtils.setField(task, "maxRetryDelaySeconds", 3600L);
        }

        private OrderTimeoutCloseFail failure() {
            return new OrderTimeoutCloseFail().setId(1L).setOrderId(10L).setFailCount(1).setMaxFailCount(5);
        }
    }
}
