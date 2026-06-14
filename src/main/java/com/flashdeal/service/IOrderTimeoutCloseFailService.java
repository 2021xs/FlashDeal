package com.flashdeal.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flashdeal.entity.OrderTimeoutCloseFail;

import java.time.LocalDateTime;
import java.util.List;

public interface IOrderTimeoutCloseFailService extends IService<OrderTimeoutCloseFail> {

    String STATUS_INIT = "INIT";
    String STATUS_PROCESSING = "PROCESSING";
    String STATUS_NEED_MANUAL = "NEED_MANUAL";
    String STATUS_HANDLED = "HANDLED";

    OrderTimeoutCloseFail recordFailure(Long orderId,
                                        Long userId,
                                        Long voucherId,
                                        String failReason,
                                        int maxFailCount,
                                        LocalDateTime nextRetryTime);

    OrderTimeoutCloseFail recordFailureWithBackoff(Long orderId,
                                                   Long userId,
                                                   Long voucherId,
                                                   String failReason,
                                                   int maxFailCount,
                                                   long baseRetryDelaySeconds,
                                                   long maxRetryDelaySeconds);

    boolean isNeedManual(Long orderId);

    boolean isRetryDue(Long orderId, LocalDateTime now);

    boolean markHandled(Long orderId);

    List<OrderTimeoutCloseFail> listRetryable(LocalDateTime now, int limit);

    boolean claimRetry(OrderTimeoutCloseFail fail);

    boolean markRetryFailed(OrderTimeoutCloseFail fail,
                            String reason,
                            long baseRetryDelaySeconds,
                            long maxRetryDelaySeconds);

    boolean markHandled(OrderTimeoutCloseFail fail);

    int recoverStuckProcessing(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit);
}
