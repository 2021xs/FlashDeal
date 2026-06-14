package com.flashdeal.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.dto.RedisStockRecoveryMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.mapper.OutboxEventMapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxEventServiceImplTest {

    @Test
    void onlyOnePublisherShouldClaimEvent() {
        OutboxEventMapper mapper = mock(OutboxEventMapper.class);
        OutboxEventServiceImpl service = new OutboxEventServiceImpl();
        ReflectionTestUtils.setField(service, "baseMapper", mapper);
        OutboxEvent event = new OutboxEvent().setId(1L).setStatus("INIT").setRetryCount(0);
        when(mapper.claimSending(1L, "INIT", 0)).thenReturn(1, 0);

        assertTrue(service.claimSending(event));
        assertFalse(service.claimSending(event));
    }

    @Test
    void orderTimeoutEventShouldContainOrderAndFixedExpireTime() throws Exception {
        OutboxEventServiceImpl service = new OutboxEventServiceImpl();
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(service, "orderTimeoutSeconds", 30L);
        ReflectionTestUtils.setField(service, "maxRetryCount", 5);
        LocalDateTime createTime = LocalDateTime.of(2026, 6, 10, 10, 0);
        VoucherOrder order = new VoucherOrder()
                .setId(10L).setUserId(20L).setVoucherId(30L).setCreateTime(createTime);

        OutboxEvent event = service.buildOrderTimeoutEvent(order);
        OrderTimeoutMessage message = objectMapper.readValue(event.getPayload(), OrderTimeoutMessage.class);

        assertEquals(32, event.getEventId().length());
        assertEquals("order:10", event.getBizKey());
        assertEquals(createTime.plusSeconds(30), event.getExpireTime());
        assertEquals(event.getExpireTime(), message.getExpireTime());
    }

    @Test
    void redisStockRecoveryEventShouldBeUniqueByOrderAndContainRecoveryPayload() throws Exception {
        OutboxEventServiceImpl service = new OutboxEventServiceImpl();
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(service, "redisStockRecoveryMaxRetryCount", 10);
        VoucherOrder order = new VoucherOrder().setId(10L).setUserId(20L).setVoucherId(30L);

        OutboxEvent event = service.buildRedisStockRecoveryEvent(order);
        RedisStockRecoveryMessage message =
                objectMapper.readValue(event.getPayload(), RedisStockRecoveryMessage.class);

        assertEquals("REDIS_STOCK_RECOVERY", event.getEventType());
        assertEquals("order:10", event.getBizKey());
        assertEquals(10L, message.getOrderId());
        assertEquals(30L, message.getVoucherId());
    }
}
