package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.service.IMqMessageService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MqMessageRetryTaskTest {

    @Test
    void retryableMessageShouldBeClaimedAsRetryingBeforeResend() throws Exception {
        Fixture fixture = new Fixture();
        MqMessage message = message();
        when(fixture.mqMessageService.listExceededRetryMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.emptyList());
        when(fixture.mqMessageService.listRetryableMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.singletonList(message));
        when(fixture.mqMessageService.markRetrying(eq(1L), anyList(), eq(0), any(LocalDateTime.class), eq("RETRYING")))
                .thenReturn(true);

        fixture.task.retrySeckillOrderMessages();

        ArgumentCaptor<LocalDateTime> nextRetryCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        ArgumentCaptor<List> statusCaptor = ArgumentCaptor.forClass(List.class);
        verify(fixture.mqMessageService).markRetrying(eq(1L), statusCaptor.capture(), eq(0), nextRetryCaptor.capture(), eq("RETRYING"));
        assertTrue(statusCaptor.getValue().contains(MqMessageStatus.INIT));
        assertTrue(!statusCaptor.getValue().contains(MqMessageStatus.RETRYING));
        assertTrue(nextRetryCaptor.getValue().isAfter(LocalDateTime.now().minusSeconds(1)));
        verify(fixture.voucherOrderProducer).resendSeckillOrder(any(VoucherOrderMessage.class));
        verify(fixture.mqMessageService, never()).scheduleNextRetry(eq(1L), anyList(), any(LocalDateTime.class), any());
    }

    @Test
    void staleRetryingMessageShouldOnlyBeClaimedFromRetryingStatus() throws Exception {
        Fixture fixture = new Fixture();
        MqMessage message = message();
        message.setStatus(MqMessageStatus.RETRYING.name());
        when(fixture.mqMessageService.listExceededRetryMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.emptyList());
        when(fixture.mqMessageService.listRetryableMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.singletonList(message));
        when(fixture.mqMessageService.markRetrying(eq(1L), anyList(), eq(0), any(LocalDateTime.class), eq("RETRYING")))
                .thenReturn(true);

        fixture.task.retrySeckillOrderMessages();

        ArgumentCaptor<List> statusCaptor = ArgumentCaptor.forClass(List.class);
        verify(fixture.mqMessageService).markRetrying(eq(1L), statusCaptor.capture(), eq(0), any(LocalDateTime.class), eq("RETRYING"));
        assertTrue(statusCaptor.getValue().contains(MqMessageStatus.RETRYING));
        assertTrue(statusCaptor.getValue().size() == 1);
        verify(fixture.voucherOrderProducer).resendSeckillOrder(any(VoucherOrderMessage.class));
    }

    @Test
    void resendFailureShouldScheduleNextRetryFromRetryingState() throws Exception {
        Fixture fixture = new Fixture();
        MqMessage message = message();
        when(fixture.mqMessageService.listExceededRetryMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.emptyList());
        when(fixture.mqMessageService.listRetryableMessages(any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.singletonList(message));
        when(fixture.mqMessageService.markRetrying(eq(1L), anyList(), eq(0), any(LocalDateTime.class), eq("RETRYING")))
                .thenReturn(true);
        doThrow(new IllegalStateException("rabbit down"))
                .when(fixture.voucherOrderProducer).resendSeckillOrder(any(VoucherOrderMessage.class));

        fixture.task.retrySeckillOrderMessages();

        ArgumentCaptor<List> statusCaptor = ArgumentCaptor.forClass(List.class);
        verify(fixture.mqMessageService).scheduleNextRetry(
                eq(1L), statusCaptor.capture(), any(LocalDateTime.class), eq("rabbit down"));
        assertTrue(statusCaptor.getValue().contains(MqMessageStatus.RETRYING));
        assertTrue(statusCaptor.getValue().size() == 1);
    }

    @Test
    void exceededRetryMessageShouldEnterNeedManualWithExpectedState() throws Exception {
        Fixture fixture = new Fixture();
        MqMessage message = message();
        message.setStatus(MqMessageStatus.CONFIRM_FAILED.name());
        message.setRetryCount(3);
        message.setMaxRetryCount(3);
        when(fixture.mqMessageService.listExceededRetryMessages(
                any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.singletonList(message));
        when(fixture.mqMessageService.listRetryableMessages(
                any(LocalDateTime.class), any(LocalDateTime.class), eq(50)))
                .thenReturn(Collections.emptyList());

        fixture.task.retrySeckillOrderMessages();

        verify(fixture.mqMessageService).markNeedManualAfterRetryExceeded(
                eq(1L), eq(MqMessageStatus.CONFIRM_FAILED), eq(3), any());
        verify(fixture.voucherOrderProducer, never()).resendSeckillOrder(any());
    }

    private static MqMessage message() throws Exception {
        VoucherOrderMessage body = new VoucherOrderMessage();
        body.setMessageId(1L);
        body.setOrderId(10L);
        body.setUserId(20L);
        body.setVoucherId(30L);

        MqMessage message = new MqMessage();
        message.setId(1L);
        message.setBizId(10L);
        message.setStatus(MqMessageStatus.INIT.name());
        message.setRetryCount(0);
        message.setMessageBody(new ObjectMapper().writeValueAsString(body));
        return message;
    }

    private static class Fixture {
        private final MqMessageRetryTask task = new MqMessageRetryTask();
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final VoucherOrderProducer voucherOrderProducer = mock(VoucherOrderProducer.class);

        private Fixture() {
            ReflectionTestUtils.setField(task, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(task, "voucherOrderProducer", voucherOrderProducer);
            ReflectionTestUtils.setField(task, "objectMapper", new ObjectMapper());
            ReflectionTestUtils.setField(task, "retryEnabled", true);
            ReflectionTestUtils.setField(task, "retryBatchSize", 50);
            ReflectionTestUtils.setField(task, "sentTimeoutSeconds", 300L);
            ReflectionTestUtils.setField(task, "nextRetryDelaySeconds", 30L);
        }
    }
}
