package com.flashdeal.service.impl;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.Result;
import com.flashdeal.dto.SeckillOrderResultDTO;
import com.flashdeal.dto.UserDTO;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.enums.VoucherOrderStatus;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VoucherOrderServiceImplSeckillResultTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void existingOrderShouldReturnSuccess() {
        Fixture fixture = new Fixture();
        VoucherOrder order = new VoucherOrder();
        order.setId(10L);
        order.setUserId(20L);
        order.setStatus(VoucherOrderStatus.UNPAID.getCode());
        doReturn(order).when(fixture.service).getById(10L);

        Result result = fixture.service.querySeckillResult(10L);

        assertTrue(result.getSuccess());
        SeckillOrderResultDTO data = (SeckillOrderResultDTO) result.getData();
        assertEquals("SUCCESS", data.getResult());
        assertEquals(VoucherOrderStatus.UNPAID.getCode(), data.getOrderStatus());
        assertEquals("订单已生成，请继续完成支付", data.getMessage());
        verify(fixture.mqMessageService, never()).query();
    }

    @Test
    void otherUserExistingOrderShouldReturnFail() {
        Fixture fixture = new Fixture();
        VoucherOrder order = new VoucherOrder();
        order.setId(10L);
        order.setUserId(99L);
        order.setStatus(VoucherOrderStatus.UNPAID.getCode());
        doReturn(order).when(fixture.service).getById(10L);

        Result result = fixture.service.querySeckillResult(10L);

        assertFalse(result.getSuccess());
        assertEquals("无权查询该订单", result.getErrorMsg());
        assertNull(result.getData());
        verify(fixture.mqMessageService, never()).query();
    }

    @Test
    void sentMessageWithoutOrderShouldReturnProcessing() throws Exception {
        Fixture fixture = new Fixture();
        doReturn(null).when(fixture.service).getById(10L);
        givenMqMessage(fixture, MqMessageStatus.SENT, 20L);

        Result result = fixture.service.querySeckillResult(10L);

        assertTrue(result.getSuccess());
        SeckillOrderResultDTO data = (SeckillOrderResultDTO) result.getData();
        assertEquals("PROCESSING", data.getResult());
        assertEquals(MqMessageStatus.SENT.name(), data.getMqStatus());
    }

    @Test
    void failedMessageWithoutOrderShouldReturnFailed() throws Exception {
        Fixture fixture = new Fixture();
        doReturn(null).when(fixture.service).getById(10L);
        givenMqMessage(fixture, MqMessageStatus.FAILED, 20L);

        Result result = fixture.service.querySeckillResult(10L);

        assertTrue(result.getSuccess());
        SeckillOrderResultDTO data = (SeckillOrderResultDTO) result.getData();
        assertEquals("FAILED", data.getResult());
        assertEquals(MqMessageStatus.FAILED.name(), data.getMqStatus());
    }

    @Test
    void otherUserMessageShouldReturnFail() throws Exception {
        Fixture fixture = new Fixture();
        doReturn(null).when(fixture.service).getById(10L);
        givenMqMessage(fixture, MqMessageStatus.SENT, 99L);

        Result result = fixture.service.querySeckillResult(10L);

        assertFalse(result.getSuccess());
        assertEquals("无权查询该订单", result.getErrorMsg());
        assertNull(result.getData());
    }

    @Test
    void missingOrderAndMessageShouldReturnUnknown() {
        Fixture fixture = new Fixture();
        doReturn(null).when(fixture.service).getById(10L);
        givenNoMqMessage(fixture);

        Result result = fixture.service.querySeckillResult(10L);

        assertTrue(result.getSuccess());
        SeckillOrderResultDTO data = (SeckillOrderResultDTO) result.getData();
        assertEquals("UNKNOWN", data.getResult());
        assertNull(data.getMqStatus());
    }

    @Test
    void unparsableMessageBodyShouldReturnUnknownWithoutMqStatus() {
        Fixture fixture = new Fixture();
        doReturn(null).when(fixture.service).getById(10L);
        givenRawMqMessage(fixture, "{bad-json", MqMessageStatus.SENT);

        Result result = fixture.service.querySeckillResult(10L);

        assertTrue(result.getSuccess());
        SeckillOrderResultDTO data = (SeckillOrderResultDTO) result.getData();
        assertEquals("UNKNOWN", data.getResult());
        assertNull(data.getMqStatus());
    }

    private static void givenMqMessage(Fixture fixture, MqMessageStatus status, Long userId) throws Exception {
        QueryChainWrapper<MqMessage> query = mock(QueryChainWrapper.class);
        when(fixture.mqMessageService.query()).thenReturn(query);
        when(query.eq(any(), any())).thenReturn(query);
        when(query.one()).thenReturn(mqMessage(status, userId));
    }

    private static void givenNoMqMessage(Fixture fixture) {
        QueryChainWrapper<MqMessage> query = mock(QueryChainWrapper.class);
        when(fixture.mqMessageService.query()).thenReturn(query);
        when(query.eq(any(), any())).thenReturn(query);
        when(query.one()).thenReturn(null);
    }

    private static void givenRawMqMessage(Fixture fixture, String body, MqMessageStatus status) {
        QueryChainWrapper<MqMessage> query = mock(QueryChainWrapper.class);
        when(fixture.mqMessageService.query()).thenReturn(query);
        when(query.eq(any(), any())).thenReturn(query);
        MqMessage message = new MqMessage();
        message.setId(1L);
        message.setBizType(IMqMessageService.SECKILL_ORDER_BIZ_TYPE);
        message.setBizId(10L);
        message.setStatus(status.name());
        message.setMessageBody(body);
        when(query.one()).thenReturn(message);
    }

    private static MqMessage mqMessage(MqMessageStatus status, Long userId) throws Exception {
        VoucherOrderMessage body = new VoucherOrderMessage();
        body.setMessageId(1L);
        body.setOrderId(10L);
        body.setUserId(userId);
        body.setVoucherId(30L);

        MqMessage message = new MqMessage();
        message.setId(1L);
        message.setBizType(IMqMessageService.SECKILL_ORDER_BIZ_TYPE);
        message.setBizId(10L);
        message.setStatus(status.name());
        message.setMessageBody(OBJECT_MAPPER.writeValueAsString(body));
        return message;
    }

    private static class Fixture {
        private final VoucherOrderServiceImpl service = spy(new VoucherOrderServiceImpl());
        private final IMqMessageService mqMessageService = mock(IMqMessageService.class);

        private Fixture() {
            UserDTO user = new UserDTO();
            user.setId(20L);
            UserHolder.saveUser(user);
            ReflectionTestUtils.setField(service, "mqMessageService", mqMessageService);
            ReflectionTestUtils.setField(service, "objectMapper", OBJECT_MAPPER);
        }
    }
}
