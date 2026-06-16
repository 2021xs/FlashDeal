package com.flashdeal.mq;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillReconcileTaskTest {

    @Test
    void existingOrderShouldBeMarkedConsumed() {
        Fixture fixture = new Fixture();
        VoucherOrder order = new VoucherOrder();
        order.setId(10L);
        when(fixture.voucherOrderService.getById(10L)).thenReturn(order);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.mqMessageService).markConsumedAfterReconcile(1L);
    }

    @Test
    void missingOrderShouldWaitForRedisMysqlReconcileWithoutRollback() {
        Fixture fixture = new Fixture();
        QueryChainWrapper<VoucherOrder> query = mock(QueryChainWrapper.class);
        when(fixture.voucherOrderService.getById(10L)).thenReturn(null);
        when(fixture.voucherOrderService.query()).thenReturn(query);
        when(query.eq(anyString(), any())).thenReturn(query);
        when(query.one()).thenReturn(null);
        when(fixture.seckillReservationService.hasPending(10L)).thenReturn(true);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.mqMessageService).markFailedAfterReconcile(1L, "WAITING_REDIS_MYSQL_RECONCILE");
        verify(fixture.seckillReservationService, never()).rollback(any(), any(), any());
    }

    @Test
    void missingOrderAndPendingAbsentShouldStayNeedManualWithoutRedisRollback() {
        Fixture fixture = new Fixture();
        QueryChainWrapper<VoucherOrder> query = mock(QueryChainWrapper.class);
        when(fixture.voucherOrderService.getById(10L)).thenReturn(null);
        when(fixture.voucherOrderService.query()).thenReturn(query);
        when(query.eq(anyString(), any())).thenReturn(query);
        when(query.one()).thenReturn(null);
        when(fixture.seckillReservationService.hasPending(10L)).thenReturn(false);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.mqMessageService).markNeedManualAfterReconcile(
                1L, "ORDER_NOT_FOUND_AND_REDIS_PENDING_ABSENT");
        verify(fixture.seckillReservationService, never()).rollback(any(), any(), any());
    }

    private static class Fixture {
        private final SeckillReconcileTask task = new SeckillReconcileTask();
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
        private final SeckillReservationService seckillReservationService = mock(SeckillReservationService.class);

        private Fixture() {
            ReflectionTestUtils.setField(task, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(task, "voucherOrderService", voucherOrderService);
            ReflectionTestUtils.setField(task, "seckillReservationService", seckillReservationService);
            ReflectionTestUtils.setField(task, "objectMapper", new ObjectMapper());
            ReflectionTestUtils.setField(task, "reconcileEnabled", true);
            ReflectionTestUtils.setField(task, "reconcileBatchSize", 100);
            when(mqMessageService.listNeedManualMessages(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, 100))
                    .thenReturn(Collections.singletonList(message()));
        }
    }

    private static MqMessage message() {
        VoucherOrderMessage body = new VoucherOrderMessage();
        body.setMessageId(1L);
        body.setOrderId(10L);
        body.setUserId(20L);
        body.setVoucherId(30L);
        MqMessage message = new MqMessage();
        message.setId(1L);
        message.setBizId(10L);
        message.setStatus("NEED_MANUAL");
        try {
            message.setMessageBody(new ObjectMapper().writeValueAsString(body));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return message;
    }
}
