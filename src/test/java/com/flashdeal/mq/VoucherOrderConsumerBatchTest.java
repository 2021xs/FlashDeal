package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.dto.SeckillReservationState;
import com.flashdeal.dto.SeckillReservationStatus;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.flashdeal.utils.RabbitConstants.SECKILL_CLAIM_RETRY_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_CLAIM_RETRY_ROUTING_KEY;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLK;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLX;

class VoucherOrderConsumerBatchTest {

    @Test
    void batchSuccessAndIdempotentSuccessShouldAck() throws Exception {
        Fixture fixture = new Fixture();
        BatchVoucherOrderResult result = new BatchVoucherOrderResult();
        result.addSuccessOrderId(10L);
        result.addIdempotentOrderId(11L);
        when(fixture.voucherOrderService.createClaimedVoucherOrdersBatch(anyList())).thenReturn(result);
        when(fixture.mqMessageService.markConsumed(1L)).thenReturn(true);
        when(fixture.mqMessageService.markConsumed(2L)).thenReturn(true);

        fixture.consumer.handleSeckillOrderBatch(Arrays.asList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L),
                message(fixture.objectMapper, 2L, 11L, 21L, 30L, 101L)
        ), fixture.channel);

        verify(fixture.voucherOrderService).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.voucherOrderService, never()).hasExistingOrder(any(), any(), any());
        verify(fixture.seckillReservationService).commit(30L, 20L, 10L);
        verify(fixture.seckillReservationService).commit(30L, 21L, 11L);
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel).basicAck(101L, false);
    }

    @Test
    void nonRetryableFailureShouldNackWithoutRetry() throws Exception {
        Fixture fixture = new Fixture();
        BatchVoucherOrderResult result = new BatchVoucherOrderResult();
        result.addNonRetryableFailedOrderId(10L, "stock is not enough");
        when(fixture.voucherOrderService.createClaimedVoucherOrdersBatch(anyList())).thenReturn(result);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.channel).basicNack(100L, false, false);
        verify(fixture.channel, never()).basicAck(100L, false);
        verify(fixture.seckillReservationService, never()).commit(any(), any(), any());
    }

    @Test
    void duplicateShouldNotCommitWhenServiceReturnsRetryableFailure() throws Exception {
        Fixture fixture = new Fixture();
        ReflectionTestUtils.setField(fixture.consumer, "retryTimes", 0);
        BatchVoucherOrderResult result = new BatchVoucherOrderResult();
        result.addRetryableFailedOrderId(10L, "database timeout");
        result.addRetryableFailedOrderId(11L, "PRIMARY_ORDER_RETRYABLE_FAILED");
        when(fixture.voucherOrderService.createClaimedVoucherOrdersBatch(anyList())).thenReturn(result);

        fixture.consumer.handleSeckillOrderBatch(Arrays.asList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L),
                message(fixture.objectMapper, 2L, 11L, 20L, 30L, 101L)
        ), fixture.channel);

        verify(fixture.seckillReservationService, never()).commit(any(), any(), any());
        verify(fixture.channel).basicNack(100L, false, false);
        verify(fixture.channel).basicNack(101L, false, false);
        verify(fixture.channel, never()).basicAck(100L, false);
        verify(fixture.channel, never()).basicAck(101L, false);
    }

    @Test
    void committedReservationClaimFailureShouldAckWithoutCreatingOrder() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.COMMITTED, 10L, System.currentTimeMillis(), "10:COMMITTED:1"));
        when(fixture.mqMessageService.markConsumed(1L)).thenReturn(true);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void processingReservationClaimFailureShouldPublishRetryAndAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.PROCESSING, 10L, System.currentTimeMillis(), "10:PROCESSING:1"));

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), any(Message.class));
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void timedOutProcessingClaimFailureShouldPublishRetryAndAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.PROCESSING, 10L,
                        System.currentTimeMillis() - 2000L, "10:PROCESSING:1"));

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), any(Message.class));
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void missingReservationClaimFailureShouldPublishRetryWithIncrementedHeaderAndAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.MISSING, null, null, null));
        Message source = message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L);
        source.getMessageProperties().setHeader("x-seckill-claim-retry-count", 2);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(source), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), captor.capture());
        Assertions.assertEquals(3, captor.getValue().getMessageProperties().getHeaders().get("x-seckill-claim-retry-count"));
        Assertions.assertEquals("MISSING", captor.getValue().getMessageProperties().getHeaders().get("x-seckill-claim-last-status"));
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void unknownReservationAfterMaxRetryShouldPublishDlqAndAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.UNKNOWN, 10L, null, "10:BAD:1"));
        Message source = message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L);
        source.getMessageProperties().setHeader("x-seckill-claim-retry-count", 5);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(source), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_ORDER_DLX), eq(SECKILL_ORDER_DLK), any(Message.class));
        verify(fixture.rabbitTemplate, never()).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), any(Message.class));
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void canceledReservationClaimFailureShouldAckWithoutRetryOrDlq() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.CANCELED, 10L, System.currentTimeMillis(), "10:CANCELED:1"));
        when(fixture.mqMessageService.markConsumed(1L)).thenReturn(true);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.rabbitTemplate, never()).convertAndSend(any(String.class), any(String.class), any(Message.class));
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void retryPublishFailureShouldFallbackRequeueWithoutAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.MISSING, null, null, null));
        doThrow(new RuntimeException("retry publish failed"))
                .when(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), any(Message.class));

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.channel).basicNack(100L, false, true);
        verify(fixture.channel, never()).basicAck(100L, false);
    }

    @Test
    void dlqPublishFailureShouldFallbackRequeueWithoutAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.UNKNOWN, 10L, null, "10:BAD:1"));
        Message source = message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L);
        source.getMessageProperties().setHeader("x-seckill-claim-retry-count", 5);
        doThrow(new RuntimeException("dlq publish failed"))
                .when(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_ORDER_DLX), eq(SECKILL_ORDER_DLK), any(Message.class));

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(source), fixture.channel);

        verify(fixture.channel).basicNack(100L, false, true);
        verify(fixture.channel, never()).basicAck(100L, false);
    }

    @Test
    void timedOutProcessingReservationShouldWaitForReconcileInsteadOfReclaim() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);
        when(fixture.seckillReservationService.getReservationState(30L, 20L))
                .thenReturn(state(SeckillReservationStatus.PROCESSING, 10L,
                        System.currentTimeMillis() - 2000L, "10:PROCESSING:1"));

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.rabbitTemplate).convertAndSend(eq(SECKILL_CLAIM_RETRY_EXCHANGE), eq(SECKILL_CLAIM_RETRY_ROUTING_KEY), any(Message.class));
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel, never()).basicNack(100L, false, true);
    }

    @Test
    void retryableFailureShouldRetryOnlyFailedSubsetAndAckLaterSuccess() throws Exception {
        Fixture fixture = new Fixture();
        ReflectionTestUtils.setField(fixture.consumer, "retryTimes", 2);
        BatchVoucherOrderResult first = new BatchVoucherOrderResult();
        first.addSuccessOrderId(10L);
        first.addRetryableFailedOrderId(11L, "deadlock");
        BatchVoucherOrderResult second = new BatchVoucherOrderResult();
        second.addSuccessOrderId(11L);
        when(fixture.voucherOrderService.createClaimedVoucherOrdersBatch(anyList()))
                .thenReturn(first)
                .thenReturn(second);
        when(fixture.mqMessageService.markConsumed(1L)).thenReturn(true);
        when(fixture.mqMessageService.markConsumed(2L)).thenReturn(true);

        fixture.consumer.handleSeckillOrderBatch(Arrays.asList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L),
                message(fixture.objectMapper, 2L, 11L, 21L, 30L, 101L)
        ), fixture.channel);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(fixture.voucherOrderService, times(2)).createClaimedVoucherOrdersBatch(captor.capture());
        Assertions.assertEquals(2, captor.getAllValues().get(0).size());
        Assertions.assertEquals(1, captor.getAllValues().get(1).size());
        verify(fixture.channel).basicAck(100L, false);
        verify(fixture.channel).basicAck(101L, false);
        verify(fixture.channel, never()).basicNack(100L, false, false);
        verify(fixture.channel, never()).basicNack(101L, false, false);
    }

    @Test
    void retryableFailureShouldNackAfterRetryExhausted() throws Exception {
        Fixture fixture = new Fixture();
        ReflectionTestUtils.setField(fixture.consumer, "retryTimes", 1);
        BatchVoucherOrderResult result = new BatchVoucherOrderResult();
        result.addRetryableFailedOrderId(10L, "database timeout");
        when(fixture.voucherOrderService.createClaimedVoucherOrdersBatch(anyList())).thenReturn(result);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, times(2)).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.channel).basicNack(100L, false, false);
        verify(fixture.channel, never()).basicAck(100L, false);
    }

    private static Message message(ObjectMapper objectMapper,
                                   Long messageId,
                                   Long orderId,
                                   Long userId,
                                   Long voucherId,
                                   long deliveryTag) throws Exception {
        VoucherOrderMessage orderMessage = new VoucherOrderMessage();
        orderMessage.setMessageId(messageId);
        orderMessage.setOrderId(orderId);
        orderMessage.setUserId(userId);
        orderMessage.setVoucherId(voucherId);
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(objectMapper.writeValueAsBytes(orderMessage), properties);
    }

    private static SeckillReservationState state(SeckillReservationStatus status, Long orderId, Long timestamp, String rawValue) {
        SeckillReservationState state = new SeckillReservationState();
        state.setStatus(status);
        state.setOrderId(orderId);
        state.setTimestamp(timestamp);
        state.setRawValue(rawValue);
        return state;
    }

    private static class Fixture {
        private final VoucherOrderConsumer consumer = new VoucherOrderConsumer();
        private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final SeckillReservationService seckillReservationService = mock(SeckillReservationService.class);
        private final RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Channel channel = mock(Channel.class);

        private Fixture() {
            ReflectionTestUtils.setField(consumer, "voucherOrderService", voucherOrderService);
            ReflectionTestUtils.setField(consumer, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(consumer, "objectMapper", objectMapper);
            ReflectionTestUtils.setField(consumer, "seckillReservationService", seckillReservationService);
            ReflectionTestUtils.setField(consumer, "rabbitTemplate", rabbitTemplate);
            ReflectionTestUtils.setField(consumer, "retryTimes", 2);
            ReflectionTestUtils.setField(consumer, "claimRetryEnabled", true);
            ReflectionTestUtils.setField(consumer, "claimRetryMaxAttempts", 5);
            when(seckillReservationService.claim(any(), any(), any())).thenReturn(true);
            when(seckillReservationService.commit(any(), any(), any())).thenReturn(1L);
        }
    }
}
