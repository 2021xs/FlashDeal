package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.BatchVoucherOrderResult;
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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    }

    @Test
    void claimFailureShouldAckWithoutCreatingOrder() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.claim(30L, 20L, 10L)).thenReturn(false);

        fixture.consumer.handleSeckillOrderBatch(Collections.singletonList(
                message(fixture.objectMapper, 1L, 10L, 20L, 30L, 100L)
        ), fixture.channel);

        verify(fixture.voucherOrderService, never()).createClaimedVoucherOrdersBatch(anyList());
        verify(fixture.channel).basicAck(100L, false);
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

    private static class Fixture {
        private final VoucherOrderConsumer consumer = new VoucherOrderConsumer();
        private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final SeckillReservationService seckillReservationService = mock(SeckillReservationService.class);
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Channel channel = mock(Channel.class);

        private Fixture() {
            ReflectionTestUtils.setField(consumer, "voucherOrderService", voucherOrderService);
            ReflectionTestUtils.setField(consumer, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(consumer, "objectMapper", objectMapper);
            ReflectionTestUtils.setField(consumer, "seckillReservationService", seckillReservationService);
            ReflectionTestUtils.setField(consumer, "retryTimes", 2);
            when(seckillReservationService.claim(any(), any(), any())).thenReturn(true);
        }
    }
}
