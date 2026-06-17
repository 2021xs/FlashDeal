package com.flashdeal.mq;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
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
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        verify(fixture.mqMessageService).markNeedManualAlerted(eq(1L), any(LocalDateTime.class), any(LocalDateTime.class));
        verify(fixture.seckillReservationService, never()).rollback(any(), any(), any());
    }

    @Test
    void orderTimeoutOutboxNeedManualShouldBeAlertedBySameTask() {
        Fixture fixture = new Fixture();
        OutboxEvent event = new OutboxEvent()
                .setId(2L)
                .setEventId("event-2")
                .setEventType(IOutboxEventService.ORDER_TIMEOUT_EVENT)
                .setBizKey("order:10")
                .setStatus("NEED_MANUAL")
                .setRetryCount(5)
                .setMaxRetryCount(5)
                .setFailReason("rabbit down");
        when(fixture.mqMessageService.listNeedManualMessages(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, 100))
                .thenReturn(Collections.emptyList());
        when(fixture.outboxEventService.listNeedManualForAlert(
                eq(IOutboxEventService.ORDER_TIMEOUT_EVENT), any(LocalDateTime.class), eq(100)))
                .thenReturn(Collections.singletonList(event));
        when(fixture.outboxEventService.markNeedManualAlerted(eq(2L), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(true);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.outboxEventService).markNeedManualAlerted(eq(2L), any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    void redisStockRecoveryOutboxNeedManualShouldBeAlertedBySameTask() {
        Fixture fixture = new Fixture();
        OutboxEvent event = new OutboxEvent()
                .setId(3L)
                .setEventId("event-3")
                .setEventType(IOutboxEventService.REDIS_STOCK_RECOVERY_EVENT)
                .setBizKey("order:10")
                .setStatus("NEED_MANUAL")
                .setRetryCount(10)
                .setMaxRetryCount(10)
                .setFailReason("redis down");
        when(fixture.mqMessageService.listNeedManualMessages(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, 100))
                .thenReturn(Collections.emptyList());
        when(fixture.outboxEventService.listNeedManualForAlert(
                eq(IOutboxEventService.REDIS_STOCK_RECOVERY_EVENT), any(LocalDateTime.class), eq(100)))
                .thenReturn(Collections.singletonList(event));
        when(fixture.outboxEventService.markNeedManualAlerted(eq(3L), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(true);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.outboxEventService).markNeedManualAlerted(eq(3L), any(LocalDateTime.class), any(LocalDateTime.class));
    }

    @Test
    void orderTimeoutCloseFailureNeedManualShouldBeAlertedBySameTask() {
        Fixture fixture = new Fixture();
        OrderTimeoutCloseFail failure = new OrderTimeoutCloseFail()
                .setId(4L)
                .setOrderId(10L)
                .setUserId(20L)
                .setVoucherId(30L)
                .setFailCount(5)
                .setMaxFailCount(5)
                .setStatus(IOrderTimeoutCloseFailService.STATUS_NEED_MANUAL)
                .setLastFailReason("mysql down");
        when(fixture.mqMessageService.listNeedManualMessages(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, 100))
                .thenReturn(Collections.emptyList());
        when(fixture.orderTimeoutCloseFailService.listNeedManualForAlert(any(LocalDateTime.class), eq(100)))
                .thenReturn(Collections.singletonList(failure));
        when(fixture.orderTimeoutCloseFailService.markNeedManualAlerted(eq(4L), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(true);

        fixture.task.reconcileNeedManualSeckillMessages();

        verify(fixture.orderTimeoutCloseFailService).markNeedManualAlerted(
                eq(4L), any(LocalDateTime.class), any(LocalDateTime.class));
    }

    private static class Fixture {
        private final SeckillReconcileTask task = new SeckillReconcileTask();
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
        private final SeckillReservationService seckillReservationService = mock(SeckillReservationService.class);
        private final IOutboxEventService outboxEventService = mock(IOutboxEventService.class);
        private final IOrderTimeoutCloseFailService orderTimeoutCloseFailService =
                mock(IOrderTimeoutCloseFailService.class);

        private Fixture() {
            ReflectionTestUtils.setField(task, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(task, "voucherOrderService", voucherOrderService);
            ReflectionTestUtils.setField(task, "seckillReservationService", seckillReservationService);
            ReflectionTestUtils.setField(task, "outboxEventService", outboxEventService);
            ReflectionTestUtils.setField(task, "orderTimeoutCloseFailService", orderTimeoutCloseFailService);
            ReflectionTestUtils.setField(task, "objectMapper", new ObjectMapper());
            ReflectionTestUtils.setField(task, "reconcileEnabled", true);
            ReflectionTestUtils.setField(task, "reconcileBatchSize", 100);
            ReflectionTestUtils.setField(task, "alertSuppressSeconds", 1800L);
            when(mqMessageService.listNeedManualMessages(IMqMessageService.SECKILL_ORDER_BIZ_TYPE, 100))
                    .thenReturn(Collections.singletonList(message()));
            when(mqMessageService.markNeedManualAlerted(any(), any(), any())).thenReturn(true);
            when(outboxEventService.listNeedManualForAlert(
                    eq(IOutboxEventService.ORDER_TIMEOUT_EVENT), any(LocalDateTime.class), eq(100)))
                    .thenReturn(Collections.emptyList());
            when(outboxEventService.listNeedManualForAlert(
                    eq(IOutboxEventService.REDIS_STOCK_RECOVERY_EVENT), any(LocalDateTime.class), eq(100)))
                    .thenReturn(Collections.emptyList());
            when(orderTimeoutCloseFailService.listNeedManualForAlert(any(LocalDateTime.class), eq(100)))
                    .thenReturn(Collections.emptyList());
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
