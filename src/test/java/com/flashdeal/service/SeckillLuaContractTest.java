package com.flashdeal.service;

import org.junit.jupiter.api.Test;
import org.springframework.util.StreamUtils;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SeckillLuaContractTest {

    @Test
    void seckillLuaShouldAtomicallyWriteReservationAndPendingEvidence() throws Exception {
        String lua = resource("seckill.lua");
        assertTrue(lua.contains("redis.call('incrby', stockKey, -1)"));
        assertTrue(lua.contains("redis.call('sadd', orderKey, userId)"));
        assertTrue(lua.contains("orderId .. ':PENDING:' .. nowMillis"));
        assertTrue(lua.contains("redis.call('zadd', pendingKey, nowMillis, orderId)"));
        assertTrue(lua.contains("redis.call('hset', pendingDetailKey"));
        assertTrue(lua.contains("'messageId', messageId"));
    }

    @Test
    void rollbackLuaShouldUseOrderAndReservationStateCas() throws Exception {
        String lua = resource("seckill_rollback.lua");
        assertTrue(lua.contains("local expectedPrefix = orderId .. ':'"));
        assertTrue(lua.contains("orderId .. ':PENDING:'"));
        assertTrue(lua.contains("orderId .. ':PROCESSING:'"));
        assertTrue(lua.contains("redis.call('del', reservationKey)"));
    }

    private String resource(String name) throws Exception {
        return StreamUtils.copyToString(
                getClass().getClassLoader().getResourceAsStream(name),
                StandardCharsets.UTF_8);
    }
}
