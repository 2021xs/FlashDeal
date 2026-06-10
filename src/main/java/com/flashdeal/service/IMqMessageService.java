package com.flashdeal.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.enums.MqMessageStatus;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface IMqMessageService extends IService<MqMessage> {

    String SECKILL_ORDER_BIZ_TYPE = "SECKILL_ORDER";

    boolean markSent(Long messageId);

    boolean markSentForRetry(Long messageId);

    boolean markConfirmed(Long messageId);

    boolean markConfirmFailed(Long messageId, String reason, LocalDateTime nextRetryTime);

    boolean markReturned(Long messageId, String reason, LocalDateTime nextRetryTime);

    boolean markConsumed(Long messageId);

    boolean markConsumedByBiz(String bizType, Long bizId);

    boolean markFailedAfterDlqRollback(Long messageId, String reason);

    boolean markFailedAfterDlqRollbackByBiz(String bizType, Long bizId, String reason);

    boolean markNeedManual(Long messageId, String reason);

    boolean markNeedManualAfterRetryExceeded(Long messageId,
                                             MqMessageStatus expectedStatus,
                                             Integer expectedRetryCount,
                                             String reason);

    boolean markConsumedAfterReconcile(Long messageId);

    boolean markFailedAfterReconcile(Long messageId, String reason);

    boolean markNeedManualAfterReconcile(Long messageId, String reason);

    boolean updateStatusIfIn(Long messageId,
                             Collection<MqMessageStatus> fromStatuses,
                             MqMessageStatus toStatus,
                             String reason,
                             LocalDateTime nextRetryTime);

    List<MqMessage> listRetryableMessages(LocalDateTime now, LocalDateTime sentBefore, int limit);

    List<MqMessage> listExceededRetryMessages(LocalDateTime now, LocalDateTime sentBefore, int limit);

    List<MqMessage> listNeedManualMessages(String bizType, int limit);

    boolean markRetrying(Long messageId,
                         Collection<MqMessageStatus> fromStatuses,
                         Integer expectedRetryCount,
                         LocalDateTime nextRetryTime,
                         String reason);

    boolean scheduleNextRetry(Long messageId,
                              Collection<MqMessageStatus> fromStatuses,
                              LocalDateTime nextRetryTime,
                              String reason);
}
