package com.flashdeal.service.impl;

import com.flashdeal.dto.SeckillStockSyncResult;
import com.flashdeal.entity.SeckillVoucher;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeckillStockSyncServiceImplTest {

    @Test
    void futureActivityShouldInitializeMissingRedisStock() {
        Fixture fixture = new Fixture();
        when(fixture.redisTemplate.hasKey("seckill:stock:10")).thenReturn(false);
        SeckillStockSyncResult result = new SeckillStockSyncResult();

        fixture.sync(voucher(LocalDateTime.now().plusHours(1)), false, result);

        verify(fixture.valueOperations).set("seckill:stock:10", "20");
        assertEquals(1, result.getSyncedCount());
        assertEquals(0, result.getSkippedCount());
    }

    @Test
    void startedActivityShouldNeverInitializeMissingRedisStock() {
        Fixture fixture = new Fixture();
        SeckillStockSyncResult result = new SeckillStockSyncResult();

        fixture.sync(voucher(LocalDateTime.now().minusMinutes(1)), false, result);

        verify(fixture.redisTemplate, never()).hasKey(anyString());
        verify(fixture.valueOperations, never()).set(anyString(), anyString());
        assertEquals(0, result.getSyncedCount());
        assertEquals(1, result.getSkippedCount());
    }

    @Test
    void forceShouldNotOverwriteStockAfterActivityStarted() {
        Fixture fixture = new Fixture();
        SeckillStockSyncResult result = new SeckillStockSyncResult();

        fixture.sync(voucher(LocalDateTime.now().minusMinutes(1)), true, result);

        verify(fixture.redisTemplate, never()).hasKey(anyString());
        verify(fixture.valueOperations, never()).set(anyString(), anyString());
        assertEquals(0, result.getSyncedCount());
        assertEquals(1, result.getSkippedCount());
    }

    @Test
    void futureActivityShouldSkipExistingStockWithoutForce() {
        Fixture fixture = new Fixture();
        when(fixture.redisTemplate.hasKey("seckill:stock:10")).thenReturn(true);
        SeckillStockSyncResult result = new SeckillStockSyncResult();

        fixture.sync(voucher(LocalDateTime.now().plusHours(1)), false, result);

        verify(fixture.valueOperations, never()).set(anyString(), anyString());
        assertEquals(0, result.getSyncedCount());
        assertEquals(1, result.getSkippedCount());
    }

    @Test
    void futureActivityShouldAllowForceOverwrite() {
        Fixture fixture = new Fixture();
        when(fixture.redisTemplate.hasKey("seckill:stock:10")).thenReturn(true);
        SeckillStockSyncResult result = new SeckillStockSyncResult();

        fixture.sync(voucher(LocalDateTime.now().plusHours(1)), true, result);

        verify(fixture.valueOperations).set("seckill:stock:10", "20");
        assertEquals(1, result.getSyncedCount());
        assertEquals(0, result.getSkippedCount());
    }

    private static SeckillVoucher voucher(LocalDateTime beginTime) {
        SeckillVoucher voucher = new SeckillVoucher();
        voucher.setVoucherId(10L);
        voucher.setStock(20);
        voucher.setBeginTime(beginTime);
        voucher.setEndTime(beginTime == null ? null : beginTime.plusHours(1));
        return voucher;
    }

    private static class Fixture {
        private final SeckillStockSyncServiceImpl service = new SeckillStockSyncServiceImpl();
        private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

        private Fixture() {
            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
            ReflectionTestUtils.setField(service, "stringRedisTemplate", redisTemplate);
        }

        private void sync(SeckillVoucher voucher, boolean force, SeckillStockSyncResult result) {
            ReflectionTestUtils.invokeMethod(service, "syncOne", voucher, force, result);
        }
    }
}
