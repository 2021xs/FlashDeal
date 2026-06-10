package com.flashdeal.mq;

import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillOrderDeadLetterConsumerTest {

    @Test
    void existingOrderShouldMarkConsumedAndAckWithoutRedisRollback() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.voucherOrderService.hasExistingOrder(10L, 20L, 30L)).thenReturn(true);

        fixture.consumer.handleDeadLetter(orderMessage(), message(), fixture.channel);

        verify(fixture.mqMessageService).markConsumed(1L);
        verify(fixture.seckillReservationService, never()).rollback(any(), any(), any());
        verify(fixture.seckillReservationService).cleanupPending(10L);
        verify(fixture.channel).basicAck(100L, false);
    }

    @Test
    void missingOrderShouldRollbackRedisAndMarkFailed() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.rollback(30L, 20L, 10L)).thenReturn(0L);

        fixture.consumer.handleDeadLetter(orderMessage(), message(), fixture.channel);

        verify(fixture.mqMessageService).markFailedAfterDlqRollback(eq(1L), any(String.class));
        verify(fixture.channel).basicAck(100L, false);
    }

    @Test
    void rollbackFailureShouldMarkNeedManualAndAck() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.seckillReservationService.rollback(30L, 20L, 10L)).thenReturn(2L);

        fixture.consumer.handleDeadLetter(orderMessage(), message(), fixture.channel);

        verify(fixture.mqMessageService).markNeedManual(eq(1L), eq("DLQ_ROLLBACK_SKIPPED"));
        verify(fixture.channel).basicAck(100L, false);
    }

    private static VoucherOrderMessage orderMessage() {
        VoucherOrderMessage orderMessage = new VoucherOrderMessage();
        orderMessage.setMessageId(1L);
        orderMessage.setOrderId(10L);
        orderMessage.setUserId(20L);
        orderMessage.setVoucherId(30L);
        return orderMessage;
    }

    private static Message message() {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(100L);
        return new Message(new byte[0], properties);
    }

    private static class Fixture {
        private final SeckillOrderDeadLetterConsumer consumer = new SeckillOrderDeadLetterConsumer();
        private final IVoucherOrderService voucherOrderService = mock(IVoucherOrderService.class);
        private final SeckillReservationService seckillReservationService = mock(SeckillReservationService.class);
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);
        private final Channel channel = mock(Channel.class);

        private Fixture() {
            ReflectionTestUtils.setField(consumer, "voucherOrderService", voucherOrderService);
            ReflectionTestUtils.setField(consumer, "seckillReservationService", seckillReservationService);
            ReflectionTestUtils.setField(consumer, "mqMessageService", mqMessageService);
        }
    }
}
