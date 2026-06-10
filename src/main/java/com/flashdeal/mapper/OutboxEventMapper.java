package com.flashdeal.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flashdeal.entity.OutboxEvent;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxEventMapper extends BaseMapper<OutboxEvent> {

    List<OutboxEvent> selectPublishable(@Param("now") LocalDateTime now, @Param("limit") int limit);

    int claimSending(@Param("id") Long id,
                     @Param("expectedStatus") String expectedStatus,
                     @Param("expectedRetryCount") Integer expectedRetryCount);

    int markSent(@Param("id") Long id, @Param("expectedRetryCount") Integer expectedRetryCount);

    int markPublishFailed(@Param("id") Long id,
                          @Param("expectedRetryCount") Integer expectedRetryCount,
                          @Param("nextRetryTime") LocalDateTime nextRetryTime,
                          @Param("failReason") String failReason);

    int recoverStuckSending(@Param("staleBefore") LocalDateTime staleBefore,
                            @Param("nextRetryTime") LocalDateTime nextRetryTime,
                            @Param("limit") int limit);

    List<OutboxEvent> selectNeedManual(@Param("limit") int limit);
}
