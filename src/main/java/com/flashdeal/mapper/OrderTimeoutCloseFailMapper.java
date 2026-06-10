package com.flashdeal.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flashdeal.entity.OrderTimeoutCloseFail;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

public interface OrderTimeoutCloseFailMapper extends BaseMapper<OrderTimeoutCloseFail> {

    int upsertFailure(@Param("orderId") Long orderId,
                      @Param("userId") Long userId,
                      @Param("voucherId") Long voucherId,
                      @Param("maxFailCount") int maxFailCount,
                      @Param("failReason") String failReason,
                      @Param("nextRetryTime") LocalDateTime nextRetryTime);
}
