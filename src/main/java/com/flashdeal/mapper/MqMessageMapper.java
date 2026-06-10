package com.flashdeal.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flashdeal.entity.MqMessage;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface MqMessageMapper extends BaseMapper<MqMessage> {

    int updateStatusIfIn(@Param("id") Long id,
                         @Param("fromStatuses") Collection<String> fromStatuses,
                         @Param("toStatus") String toStatus,
                         @Param("failReason") String failReason,
                         @Param("nextRetryTime") LocalDateTime nextRetryTime);

    int updateStatusByBizIfIn(@Param("bizType") String bizType,
                              @Param("bizId") Long bizId,
                              @Param("fromStatuses") Collection<String> fromStatuses,
                              @Param("toStatus") String toStatus,
                              @Param("failReason") String failReason,
                              @Param("nextRetryTime") LocalDateTime nextRetryTime);

    int markRetrying(@Param("id") Long id,
                     @Param("fromStatuses") Collection<String> fromStatuses,
                     @Param("expectedRetryCount") Integer expectedRetryCount,
                     @Param("nextRetryTime") LocalDateTime nextRetryTime,
                     @Param("failReason") String failReason);

    int scheduleNextRetry(@Param("id") Long id,
                          @Param("fromStatuses") Collection<String> fromStatuses,
                          @Param("nextRetryTime") LocalDateTime nextRetryTime,
                          @Param("failReason") String failReason);

    int markNeedManualAfterRetryExceeded(@Param("id") Long id,
                                         @Param("expectedStatus") String expectedStatus,
                                         @Param("expectedRetryCount") Integer expectedRetryCount,
                                         @Param("failReason") String failReason);

    List<MqMessage> selectRetryableMessages(@Param("statuses") Collection<String> statuses,
                                            @Param("now") LocalDateTime now,
                                            @Param("sentBefore") LocalDateTime sentBefore,
                                            @Param("limit") int limit);

    List<MqMessage> selectExceededRetryMessages(@Param("statuses") Collection<String> statuses,
                                                @Param("now") LocalDateTime now,
                                                @Param("sentBefore") LocalDateTime sentBefore,
                                                @Param("limit") int limit);
}
