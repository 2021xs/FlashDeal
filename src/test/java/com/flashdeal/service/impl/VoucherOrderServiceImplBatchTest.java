package com.flashdeal.service.impl;

import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.mapper.SeckillVoucherMapper;
import com.flashdeal.mapper.VoucherOrderMapper;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IOutboxEventService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderServiceImplBatchTest {

    @Test
    void duplicateInsideBatchShouldAckDuplicateAndInsertOnlyOne() {
        Fixture fixture = new Fixture();
        givenTransactionExecutes(fixture);
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 1)).thenReturn(1);
        when(fixture.voucherOrderMapper.insertBatch(anyList())).thenReturn(1);

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Arrays.asList(
                order(10L, 20L, 30L),
                order(11L, 20L, 30L)
        ));

        Assertions.assertTrue(result.getAckOrderIds().contains(10L));
        Assertions.assertTrue(result.getAckOrderIds().contains(11L));
        Assertions.assertTrue(result.getSuccessOrderIds().contains(10L));
        Assertions.assertTrue(result.getIdempotentOrderIds().contains(11L));
        ArgumentCaptor<List<VoucherOrder>> captor = ArgumentCaptor.forClass(List.class);
        verify(fixture.voucherOrderMapper).insertBatch(captor.capture());
        Assertions.assertEquals(1, captor.getValue().size());
        Assertions.assertEquals(10L, captor.getValue().get(0).getId());
    }

    @Test
    void existingDatabaseOrderShouldBeAckedWithoutInsert() {
        Fixture fixture = new Fixture();
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList()))
                .thenReturn(Collections.singletonList(order(null, 20L, 30L)));

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Collections.singletonList(
                order(10L, 20L, 30L)
        ));

        Assertions.assertTrue(result.getAckOrderIds().contains(10L));
        Assertions.assertTrue(result.getIdempotentOrderIds().contains(10L));
        verify(fixture.voucherOrderMapper, never()).insertBatch(anyList());
        verify(fixture.seckillVoucherMapper, never()).decrementStockBatch(any(), any(Integer.class));
    }

    @Test
    void duplicateKeyDuringBatchInsertShouldFallbackOneByOne() {
        Fixture fixture = new Fixture();
        doThrow(new DuplicateKeyException("duplicate"))
                .doAnswer(invocation -> {
                    Consumer consumer = invocation.getArgument(0);
                    consumer.accept(null);
                    return null;
                })
                .when(fixture.transactionTemplate).executeWithoutResult(any());
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        doNothing().when(fixture.service).createVoucherOrder(any(VoucherOrder.class));

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Collections.singletonList(
                order(10L, 20L, 30L)
        ));

        Assertions.assertTrue(result.getAckOrderIds().contains(10L));
        Assertions.assertTrue(result.getSuccessOrderIds().contains(10L));
        verify(fixture.service).createVoucherOrder(any(VoucherOrder.class));
    }

    @Test
    void duplicateKeyAfterBatchStockDecrementShouldFallbackAfterTransactionException() {
        Fixture fixture = new Fixture();
        doAnswer(invocation -> {
            Consumer consumer = invocation.getArgument(0);
            consumer.accept(null);
            return null;
        }).when(fixture.transactionTemplate).executeWithoutResult(any());
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 1)).thenReturn(1);
        when(fixture.voucherOrderMapper.insertBatch(anyList())).thenThrow(new DuplicateKeyException("duplicate"));
        doNothing().when(fixture.service).createVoucherOrder(any(VoucherOrder.class));

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Collections.singletonList(
                order(10L, 20L, 30L)
        ));

        Assertions.assertTrue(result.getSuccessOrderIds().contains(10L));
        verify(fixture.seckillVoucherMapper).decrementStockBatch(30L, 1);
        verify(fixture.voucherOrderMapper).insertBatch(anyList());
        verify(fixture.service).createVoucherOrder(any(VoucherOrder.class));
    }

    @Test
    void batchTransactionRuntimeExceptionShouldReturnRetryableFailure() {
        Fixture fixture = new Fixture();
        doThrow(new RuntimeException("database unavailable"))
                .when(fixture.transactionTemplate).executeWithoutResult(any());
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Collections.singletonList(
                order(10L, 20L, 30L)
        ));

        Assertions.assertTrue(result.getRetryableFailedOrderIds().contains(10L));
        Assertions.assertTrue(result.shouldRetry());
        Assertions.assertFalse(result.getNonRetryableFailedOrderIds().contains(10L));
    }

    @Test
    void batchStockNotEnoughShouldFallbackOneByOneAndClassifyStockNotEnoughAsNonRetryable() {
        Fixture fixture = new Fixture();
        givenTransactionExecutes(fixture);
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 2)).thenReturn(0);
        doNothing().when(fixture.service).createVoucherOrder(argThat(order -> order != null && order.getId().equals(10L)));
        doThrow(new IllegalStateException("Seckill voucher stock is not enough, voucherId=30"))
                .when(fixture.service).createVoucherOrder(argThat(order -> order != null && order.getId().equals(11L)));

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(Arrays.asList(
                order(10L, 20L, 30L),
                order(11L, 21L, 30L)
        ));

        Assertions.assertTrue(result.getSuccessOrderIds().contains(10L));
        Assertions.assertTrue(result.getNonRetryableFailedOrderIds().contains(11L));
        Assertions.assertFalse(result.getRetryableFailedOrderIds().contains(11L));
        Assertions.assertFalse(result.shouldRetry());
        verify(fixture.voucherOrderMapper, never()).insertBatch(anyList());
        verify(fixture.service).createVoucherOrder(argThat(order -> order != null && order.getId().equals(10L)));
        verify(fixture.service).createVoucherOrder(argThat(order -> order != null && order.getId().equals(11L)));
    }

    @Test
    void claimedBatchShouldMarkMessageConsumedInsideOrderTransaction() {
        Fixture fixture = new Fixture();
        givenTransactionExecutes(fixture);
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 1)).thenReturn(1);
        when(fixture.voucherOrderMapper.insertBatch(anyList())).thenReturn(1);
        when(fixture.mqMessageService.markConsumed(1L)).thenReturn(true);
        VoucherOrderMessage message = new VoucherOrderMessage();
        message.setMessageId(1L);
        message.setOrderId(10L);
        message.setUserId(20L);
        message.setVoucherId(30L);

        BatchVoucherOrderResult result = fixture.service.createClaimedVoucherOrdersBatch(
                Collections.singletonList(message));

        Assertions.assertTrue(result.getSuccessOrderIds().contains(10L));
        verify(fixture.mqMessageService).markConsumed(1L);
        verify(fixture.outboxEventService).saveOrderTimeoutEvents(anyList());
    }

    @Test
    void outboxFailureShouldFailOrderTransactionResult() {
        Fixture fixture = new Fixture();
        givenTransactionExecutes(fixture);
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 1)).thenReturn(1);
        when(fixture.voucherOrderMapper.insertBatch(anyList())).thenReturn(1);
        doThrow(new IllegalStateException("outbox unavailable"))
                .when(fixture.outboxEventService).saveOrderTimeoutEvents(anyList());

        BatchVoucherOrderResult result = fixture.service.createVoucherOrdersBatch(
                Collections.singletonList(order(10L, 20L, 30L)));

        Assertions.assertTrue(result.getRetryableFailedOrderIds().contains(10L));
    }

    @Test
    void orderInsertFailureShouldNotWriteOutboxEvent() {
        Fixture fixture = new Fixture();
        givenTransactionExecutes(fixture);
        when(fixture.voucherOrderMapper.selectExistingUserVoucherPairs(anyList())).thenReturn(Collections.emptyList());
        when(fixture.seckillVoucherMapper.decrementStockBatch(30L, 1)).thenReturn(1);
        when(fixture.voucherOrderMapper.insertBatch(anyList())).thenThrow(new IllegalStateException("insert failed"));

        fixture.service.createVoucherOrdersBatch(Collections.singletonList(order(10L, 20L, 30L)));

        verify(fixture.outboxEventService, never()).saveOrderTimeoutEvents(anyList());
    }

    private static void givenTransactionExecutes(Fixture fixture) {
        doAnswer(invocation -> {
            Consumer consumer = invocation.getArgument(0);
            consumer.accept(null);
            return null;
        }).when(fixture.transactionTemplate).executeWithoutResult(any());
    }

    private static VoucherOrder order(Long orderId, Long userId, Long voucherId) {
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        return order;
    }

    private static class Fixture {
        private final VoucherOrderServiceImpl service = spy(new VoucherOrderServiceImpl());
        private final VoucherOrderMapper voucherOrderMapper = mock(VoucherOrderMapper.class);
        private final SeckillVoucherMapper seckillVoucherMapper = mock(SeckillVoucherMapper.class);
        private final TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final IOutboxEventService outboxEventService = mock(IOutboxEventService.class);

        private Fixture() {
            ReflectionTestUtils.setField(service, "baseMapper", voucherOrderMapper);
            ReflectionTestUtils.setField(service, "seckillVoucherMapper", seckillVoucherMapper);
            ReflectionTestUtils.setField(service, "transactionTemplate", transactionTemplate);
            ReflectionTestUtils.setField(service, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(service, "outboxEventService", outboxEventService);
        }
    }
}
