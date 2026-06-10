package com.flashdeal.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flashdeal.entity.OrderTimeoutCloseFail;

import java.time.LocalDateTime;

public interface IOrderTimeoutCloseFailService extends IService<OrderTimeoutCloseFail> {

    String STATUS_INIT = "INIT";
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
}
