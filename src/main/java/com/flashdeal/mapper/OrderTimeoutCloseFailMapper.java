package com.flashdeal.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OrderTimeoutCloseFailMapper extends BaseMapper<OrderTimeoutCloseFail> {

    int upsertFailure(@Param("orderId") Long orderId,
                      @Param("userId") Long userId,
                      @Param("voucherId") Long voucherId,
                      @Param("maxFailCount") int maxFailCount,
                      @Param("failReason") String failReason,
                      @Param("nextRetryTime") LocalDateTime nextRetryTime);

    List<OrderTimeoutCloseFail> selectRetryable(@Param("now") LocalDateTime now, @Param("limit") int limit);

    int claimRetry(@Param("id") Long id, @Param("expectedFailCount") Integer expectedFailCount);

    int markRetryFailed(@Param("id") Long id,
                        @Param("expectedFailCount") Integer expectedFailCount,
                        @Param("nextRetryTime") LocalDateTime nextRetryTime,
                        @Param("failReason") String failReason);

    int markHandled(@Param("id") Long id, @Param("expectedFailCount") Integer expectedFailCount);

    int recoverStuckProcessing(@Param("staleBefore") LocalDateTime staleBefore,
                               @Param("nextRetryTime") LocalDateTime nextRetryTime,
                               @Param("limit") int limit);
}
