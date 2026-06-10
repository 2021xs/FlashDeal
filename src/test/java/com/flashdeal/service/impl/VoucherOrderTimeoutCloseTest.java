package com.flashdeal.service.impl;

import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.enums.VoucherOrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import static com.flashdeal.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_STOCK_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderTimeoutCloseTest {

    @Test
    void unpaidOrderShouldBeCanceledAndRecoverMysqlAndRedisStockWithoutDeletingUserMark() {
        Fixture fixture = new Fixture();
        VoucherOrder order = order(VoucherOrderStatus.UNPAID);
        doReturn(order).when(fixture.service).getById(10L);
        doReturn(true).when(fixture.service).cancelUnpaidOrder(10L);
        doReturn(true).when(fixture.service).recoverMysqlStock(30L);

        boolean closed = fixture.service.closeUnpaidOrderIfNecessary(10L);

        assertTrue(closed);
        verify(fixture.service).cancelUnpaidOrder(10L);
        verify(fixture.service).recoverMysqlStock(30L);
        verify(fixture.valueOperations).increment(SECKILL_STOCK_KEY + 30L);
        verify(fixture.stringRedisTemplate, never()).delete(startsWith(SECKILL_ORDER_KEY));
    }

    @Test
    void paidOrderShouldNotBeCanceledOrRecoverStock() {
        Fixture fixture = new Fixture();
        doReturn(order(VoucherOrderStatus.PAID)).when(fixture.service).getById(10L);

        boolean closed = fixture.service.closeUnpaidOrderIfNecessary(10L);

        assertFalse(closed);
        verify(fixture.service, never()).cancelUnpaidOrder(10L);
        verify(fixture.service, never()).recoverMysqlStock(30L);
        verify(fixture.valueOperations, never()).increment(SECKILL_STOCK_KEY + 30L);
    }

    @Test
    void repeatedCloseShouldRecoverStockOnlyOnce() {
        Fixture fixture = new Fixture();
        doReturn(order(VoucherOrderStatus.UNPAID), order(VoucherOrderStatus.CANCELED))
                .when(fixture.service).getById(10L);
        doReturn(true).when(fixture.service).cancelUnpaidOrder(10L);
        doReturn(true).when(fixture.service).recoverMysqlStock(30L);

        assertTrue(fixture.service.closeUnpaidOrderIfNecessary(10L));
        assertFalse(fixture.service.closeUnpaidOrderIfNecessary(10L));

        verify(fixture.service, times(1)).cancelUnpaidOrder(10L);
        verify(fixture.service, times(1)).recoverMysqlStock(30L);
        verify(fixture.valueOperations, times(1)).increment(SECKILL_STOCK_KEY + 30L);
    }

    @Test
    void changedStatusDuringConditionalUpdateShouldNotRecoverStock() {
        Fixture fixture = new Fixture();
        doReturn(order(VoucherOrderStatus.UNPAID)).when(fixture.service).getById(10L);
        doReturn(false).when(fixture.service).cancelUnpaidOrder(10L);

        boolean closed = fixture.service.closeUnpaidOrderIfNecessary(10L);

        assertFalse(closed);
        verify(fixture.service, never()).recoverMysqlStock(30L);
        verify(fixture.valueOperations, never()).increment(SECKILL_STOCK_KEY + 30L);
    }

    @Test
    void redisRecoverFailureShouldNotThrowAfterMysqlCloseSucceeded() {
        Fixture fixture = new Fixture();
        doReturn(order(VoucherOrderStatus.UNPAID)).when(fixture.service).getById(10L);
        doReturn(true).when(fixture.service).cancelUnpaidOrder(10L);
        doReturn(true).when(fixture.service).recoverMysqlStock(30L);
        doThrow(new RuntimeException("redis down")).when(fixture.valueOperations).increment(SECKILL_STOCK_KEY + 30L);

        boolean closed = fixture.service.closeUnpaidOrderIfNecessary(10L);

        assertTrue(closed);
        verify(fixture.service).recoverMysqlStock(30L);
        verify(fixture.valueOperations).increment(SECKILL_STOCK_KEY + 30L);
    }

    private static VoucherOrder order(VoucherOrderStatus status) {
        VoucherOrder order = new VoucherOrder();
        order.setId(10L);
        order.setUserId(20L);
        order.setVoucherId(30L);
        order.setStatus(status.getCode());
        return order;
    }

    private static class Fixture {
        private final VoucherOrderServiceImpl service = spy(new VoucherOrderServiceImpl());
        private final StringRedisTemplate stringRedisTemplate = mock(StringRedisTemplate.class);
        private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        private final TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);

        private Fixture() {
            when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
            when(transactionTemplate.execute(any())).thenAnswer(invocation ->
                    ((TransactionCallback<?>) invocation.getArgument(0)).doInTransaction(null));
            ReflectionTestUtils.setField(service, "stringRedisTemplate", stringRedisTemplate);
            ReflectionTestUtils.setField(service, "transactionTemplate", transactionTemplate);
        }
    }
}
