package com.flashdeal.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.mapper.OrderTimeoutCloseFailMapper;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OrderTimeoutCloseFailServiceImpl
        extends ServiceImpl<OrderTimeoutCloseFailMapper, OrderTimeoutCloseFail>
        implements IOrderTimeoutCloseFailService {

    private static final int MAX_REASON_LENGTH = 512;

    @Override
    public OrderTimeoutCloseFail recordFailure(Long orderId,
                                               Long userId,
                                               Long voucherId,
                                               String failReason,
                                               int maxFailCount,
                                               LocalDateTime nextRetryTime) {
        int safeMaxFailCount = maxFailCount <= 0 ? 3 : maxFailCount;
        baseMapper.upsertFailure(orderId, userId, voucherId, safeMaxFailCount, limitReason(failReason), nextRetryTime);
        return query().eq("order_id", orderId).one();
    }

    @Override
    public OrderTimeoutCloseFail recordFailureWithBackoff(Long orderId,
                                                          Long userId,
                                                          Long voucherId,
                                                          String failReason,
                                                          int maxFailCount,
                                                          long baseRetryDelaySeconds,
                                                          long maxRetryDelaySeconds) {
        int safeMaxFailCount = maxFailCount <= 0 ? 3 : maxFailCount;
        long safeBaseDelay = baseRetryDelaySeconds <= 0 ? 60 : baseRetryDelaySeconds;
        long safeMaxDelay = maxRetryDelaySeconds <= 0 ? safeBaseDelay : maxRetryDelaySeconds;
        OrderTimeoutCloseFail existing = query().eq("order_id", orderId).one();
        int currentFailCount = existing == null || existing.getFailCount() == null ? 0 : existing.getFailCount();
        int nextFailCount = currentFailCount + 1;
        LocalDateTime nextRetryTime = nextFailCount >= safeMaxFailCount
                ? null
                : LocalDateTime.now().plusSeconds(calculateBackoffSeconds(nextFailCount, safeBaseDelay, safeMaxDelay));
        return recordFailure(orderId, userId, voucherId, failReason, safeMaxFailCount, nextRetryTime);
    }

    @Override
    public boolean isNeedManual(Long orderId) {
        OrderTimeoutCloseFail fail = query()
                .eq("order_id", orderId)
                .eq("status", STATUS_NEED_MANUAL)
                .one();
        return fail != null;
    }

    @Override
    public boolean isRetryDue(Long orderId, LocalDateTime now) {
        OrderTimeoutCloseFail fail = query().eq("order_id", orderId).one();
        if (fail == null) {
            return true;
        }
        if (STATUS_NEED_MANUAL.equals(fail.getStatus())) {
            return false;
        }
        LocalDateTime nextRetryTime = fail.getNextRetryTime();
        return nextRetryTime == null || !nextRetryTime.isAfter(now);
    }

    @Override
    public boolean markHandled(Long orderId) {
        return update()
                .set("status", STATUS_HANDLED)
                .set("next_retry_time", null)
                .eq("order_id", orderId)
                .update();
    }

    @Override
    public List<OrderTimeoutCloseFail> listRetryable(LocalDateTime now, int limit) {
        return baseMapper.selectRetryable(now, Math.max(1, limit));
    }

    @Override
    public boolean claimRetry(OrderTimeoutCloseFail fail) {
        return baseMapper.claimRetry(fail.getId(), fail.getFailCount()) > 0;
    }

    @Override
    public boolean markRetryFailed(OrderTimeoutCloseFail fail,
                                   String reason,
                                   long baseRetryDelaySeconds,
                                   long maxRetryDelaySeconds) {
        int nextFailCount = (fail.getFailCount() == null ? 0 : fail.getFailCount()) + 1;
        long safeBaseDelay = baseRetryDelaySeconds <= 0 ? 60 : baseRetryDelaySeconds;
        long safeMaxDelay = maxRetryDelaySeconds <= 0 ? safeBaseDelay : maxRetryDelaySeconds;
        LocalDateTime nextRetryTime = LocalDateTime.now()
                .plusSeconds(calculateBackoffSeconds(nextFailCount, safeBaseDelay, safeMaxDelay));
        return baseMapper.markRetryFailed(
                fail.getId(), fail.getFailCount(), nextRetryTime, limitReason(reason)) > 0;
    }

    @Override
    public boolean markHandled(OrderTimeoutCloseFail fail) {
        return baseMapper.markHandled(fail.getId(), fail.getFailCount()) > 0;
    }

    @Override
    public int recoverStuckProcessing(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit) {
        return baseMapper.recoverStuckProcessing(staleBefore, nextRetryTime, Math.max(1, limit));
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= MAX_REASON_LENGTH ? reason : reason.substring(0, MAX_REASON_LENGTH);
    }

    private long calculateBackoffSeconds(int failCount, long baseDelaySeconds, long maxDelaySeconds) {
        long delay = baseDelaySeconds;
        for (int i = 1; i < failCount; i++) {
            if (delay >= maxDelaySeconds) {
                return maxDelaySeconds;
            }
            delay = delay * 2;
        }
        return Math.min(delay, maxDelaySeconds);
    }
}
