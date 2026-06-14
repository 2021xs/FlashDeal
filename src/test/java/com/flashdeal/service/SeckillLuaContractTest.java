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

    @Test
    void claimLuaShouldAllowProcessingTimeoutReclaim() throws Exception {
        String lua = resource("seckill_reservation_claim.lua");
        assertTrue(lua.contains("local processingPrefix = ARGV[3] .. ':PROCESSING:'"));
        assertTrue(lua.contains("nowMillis - processingTime >= processingTimeoutMillis"));
        assertTrue(lua.contains("redis.call('set', reservationKey, ARGV[3] .. ':PROCESSING:' .. ARGV[4])"));
    }

    @Test
    void commitLuaShouldMarkReservationCommittedAndCleanPendingEvidence() throws Exception {
        String lua = resource("seckill_reservation_commit.lua");
        assertTrue(lua.contains("orderId .. ':COMMITTED:' .. nowMillis"));
        assertTrue(lua.contains("redis.call('zrem', pendingKey, orderId)"));
        assertTrue(lua.contains("redis.call('del', pendingDetailKey)"));
        assertTrue(lua.contains("return 1"));
        assertTrue(lua.contains("return 0"));
    }

    private String resource(String name) throws Exception {
        return StreamUtils.copyToString(
                getClass().getClassLoader().getResourceAsStream(name),
                StandardCharsets.UTF_8);
    }
}
