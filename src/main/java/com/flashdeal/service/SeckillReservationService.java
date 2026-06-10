package com.flashdeal.service;

import com.flashdeal.dto.SeckillPendingDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.flashdeal.utils.RedisConstants.SECKILL_PENDING_DETAIL_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_PENDING_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_RESERVATION_KEY;

@Slf4j
@Service
public class SeckillReservationService {

    private static final DefaultRedisScript<Long> CLAIM_SCRIPT = script("seckill_reservation_claim.lua");
    private static final DefaultRedisScript<Long> ROLLBACK_SCRIPT = script("seckill_rollback.lua");
    private static final DefaultRedisScript<Long> CLEANUP_SCRIPT = script("seckill_pending_cleanup.lua");

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Value("${seckill.redis-reconcile.processing-timeout-seconds:600}")
    private long processingTimeoutSeconds;

    public boolean claim(Long voucherId, Long userId, Long orderId) {
        Long result = stringRedisTemplate.execute(
                CLAIM_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), orderId.toString(), String.valueOf(System.currentTimeMillis()));
        return result != null && result == 1L;
    }

    public Long rollback(Long voucherId, Long userId, Long orderId) {
        return rollback(voucherId, userId, orderId, System.currentTimeMillis());
    }

    public Long rollback(Long voucherId, Long userId, Long orderId, long nowMillis) {
        return stringRedisTemplate.execute(
                ROLLBACK_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString(),
                String.valueOf(nowMillis),
                String.valueOf(Math.max(0L, processingTimeoutSeconds) * 1000L));
    }

    public void cleanupPending(Long orderId) {
        stringRedisTemplate.execute(CLEANUP_SCRIPT, Collections.emptyList(), orderId.toString());
    }

    public void removePendingMember(Long orderId) {
        stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_KEY, orderId.toString());
    }

    public boolean hasPending(Long orderId) {
        return stringRedisTemplate.opsForZSet().score(SECKILL_PENDING_KEY, orderId.toString()) != null;
    }

    public Set<String> listExpiredPending(long maxScore, int limit) {
        return stringRedisTemplate.opsForZSet().rangeByScore(
                SECKILL_PENDING_KEY, 0, maxScore, 0, Math.max(1, limit));
    }

    public SeckillPendingDetail getPendingDetail(Long orderId) {
        Map<Object, Object> values = stringRedisTemplate.opsForHash().entries(SECKILL_PENDING_DETAIL_KEY + orderId);
        if (values == null || values.isEmpty()) {
            return null;
        }
        SeckillPendingDetail detail = new SeckillPendingDetail();
        detail.setVoucherId(parseLong(values.get("voucherId")));
        detail.setUserId(parseLong(values.get("userId")));
        detail.setOrderId(parseLong(values.get("orderId")));
        detail.setMessageId(parseLong(values.get("messageId")));
        detail.setCreateTime(parseLong(values.get("createTime")));
        return detail;
    }

    public String getReservation(Long voucherId, Long userId) {
        return stringRedisTemplate.opsForValue().get(SECKILL_RESERVATION_KEY + voucherId + ":" + userId);
    }

    public long getProcessingTimeoutMillis() {
        return Math.max(0L, processingTimeoutSeconds) * 1000L;
    }

    private static DefaultRedisScript<Long> script(String location) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(Long.class);
        return script;
    }

    private Long parseLong(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException e) {
            log.warn("Invalid seckill pending detail number, value={}", value);
            return null;
        }
    }
}
