package com.flashdeal.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.mapper.MqMessageMapper;
import com.flashdeal.service.IMqMessageService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class MqMessageServiceImpl extends ServiceImpl<MqMessageMapper, MqMessage> implements IMqMessageService {

    @Override
    public boolean markSent(Long messageId) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT),
                MqMessageStatus.SENT,
                null,
                null);
    }

    @Override
    public boolean markSentForRetry(Long messageId) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.RETRYING),
                MqMessageStatus.SENT,
                null,
                null);
    }

    @Override
    public boolean markSendFailed(Long messageId, String reason, LocalDateTime nextRetryTime) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT),
                MqMessageStatus.SEND_FAILED,
                reason,
                nextRetryTime);
    }

    @Override
    public boolean markConfirmed(Long messageId) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.RETRYING,
                        MqMessageStatus.SEND_FAILED, MqMessageStatus.CONFIRM_FAILED),
                MqMessageStatus.CONFIRMED,
                null,
                null);
    }

    @Override
    public boolean markConfirmFailed(Long messageId, String reason, LocalDateTime nextRetryTime) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.RETRYING,
                        MqMessageStatus.SEND_FAILED, MqMessageStatus.CONFIRM_FAILED),
                MqMessageStatus.CONFIRM_FAILED,
                reason,
                nextRetryTime);
    }

    @Override
    public boolean markReturned(Long messageId, String reason, LocalDateTime nextRetryTime) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.CONFIRMED,
                        MqMessageStatus.RETRYING, MqMessageStatus.SEND_FAILED, MqMessageStatus.CONFIRM_FAILED,
                        MqMessageStatus.RETURNED),
                MqMessageStatus.RETURNED,
                reason,
                nextRetryTime);
    }

    @Override
    public boolean markConsumed(Long messageId) {
        boolean updated = updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.CONFIRMED, MqMessageStatus.SENT, MqMessageStatus.RETRYING,
                        MqMessageStatus.SEND_FAILED, MqMessageStatus.RETURNED, MqMessageStatus.CONFIRM_FAILED),
                MqMessageStatus.CONSUMED,
                null,
                null);
        if (updated) {
            return true;
        }
        MqMessage message = getById(messageId);
        return message != null && MqMessageStatus.CONSUMED.name().equals(message.getStatus());
    }

    @Override
    public boolean markConsumedByBiz(String bizType, Long bizId) {
        boolean updated = updateStatusByBizIfIn(bizType, bizId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.CONFIRMED, MqMessageStatus.SENT, MqMessageStatus.RETRYING,
                        MqMessageStatus.SEND_FAILED, MqMessageStatus.RETURNED, MqMessageStatus.CONFIRM_FAILED),
                MqMessageStatus.CONSUMED,
                null,
                null);
        if (updated) {
            return true;
        }
        return query().eq("biz_type", bizType)
                .eq("biz_id", bizId)
                .eq("status", MqMessageStatus.CONSUMED.name())
                .count() > 0;
    }

    @Override
    public boolean markFailedAfterDlqInspection(Long messageId, String reason) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.RETRYING, MqMessageStatus.SEND_FAILED,
                        MqMessageStatus.CONFIRM_FAILED, MqMessageStatus.RETURNED, MqMessageStatus.CONFIRMED),
                MqMessageStatus.FAILED,
                reason,
                null);
    }

    @Override
    public boolean markFailedAfterDlqInspectionByBiz(String bizType, Long bizId, String reason) {
        return updateStatusByBizIfIn(bizType, bizId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.RETRYING, MqMessageStatus.SEND_FAILED,
                        MqMessageStatus.CONFIRM_FAILED, MqMessageStatus.RETURNED, MqMessageStatus.CONFIRMED),
                MqMessageStatus.FAILED,
                reason,
                null);
    }

    @Override
    public boolean markNeedManual(Long messageId, String reason) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SENT, MqMessageStatus.RETRYING, MqMessageStatus.SEND_FAILED,
                        MqMessageStatus.CONFIRM_FAILED, MqMessageStatus.RETURNED),
                MqMessageStatus.NEED_MANUAL,
                reason,
                null);
    }

    @Override
    public boolean markNeedManualAfterRetryExceeded(Long messageId,
                                                    MqMessageStatus expectedStatus,
                                                    Integer expectedRetryCount,
                                                    String reason) {
        return baseMapper.markNeedManualAfterRetryExceeded(
                messageId, expectedStatus.name(), expectedRetryCount, reason) > 0;
    }

    @Override
    public boolean markConsumedAfterReconcile(Long messageId) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.NEED_MANUAL),
                MqMessageStatus.CONSUMED,
                "RECONCILE_ORDER_EXISTS",
                null);
    }

    @Override
    public boolean markFailedAfterReconcile(Long messageId, String reason) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.NEED_MANUAL),
                MqMessageStatus.FAILED,
                reason,
                null);
    }

    @Override
    public boolean markNeedManualAfterReconcile(Long messageId, String reason) {
        return updateStatusIfIn(messageId,
                Arrays.asList(MqMessageStatus.NEED_MANUAL),
                MqMessageStatus.NEED_MANUAL,
                reason,
                null);
    }

    @Override
    public boolean updateStatusIfIn(Long messageId,
                                    Collection<MqMessageStatus> fromStatuses,
                                    MqMessageStatus toStatus,
                                    String reason,
                                    LocalDateTime nextRetryTime) {
        return baseMapper.updateStatusIfIn(messageId, toCodes(fromStatuses), toStatus.name(), reason, nextRetryTime) > 0;
    }

    @Override
    public List<MqMessage> listRetryableMessages(LocalDateTime now, LocalDateTime sentBefore, int limit) {
        return baseMapper.selectRetryableMessages(
                toCodes(Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SEND_FAILED, MqMessageStatus.RETRYING,
                        MqMessageStatus.CONFIRM_FAILED, MqMessageStatus.RETURNED)),
                now,
                sentBefore,
                limit);
    }

    @Override
    public List<MqMessage> listExceededRetryMessages(LocalDateTime now, LocalDateTime sentBefore, int limit) {
        return baseMapper.selectExceededRetryMessages(
                toCodes(Arrays.asList(MqMessageStatus.INIT, MqMessageStatus.SEND_FAILED, MqMessageStatus.RETRYING,
                        MqMessageStatus.CONFIRM_FAILED, MqMessageStatus.RETURNED)),
                now,
                sentBefore,
                limit);
    }

    @Override
    public List<MqMessage> listNeedManualMessages(String bizType, int limit) {
        int safeLimit = Math.max(1, limit);
        return query()
                .eq("biz_type", bizType)
                .eq("status", MqMessageStatus.NEED_MANUAL.name())
                .orderByAsc("update_time")
                .last("LIMIT " + safeLimit)
                .list();
    }

    @Override
    public boolean markNeedManualAlerted(Long messageId, LocalDateTime alertTime, LocalDateTime suppressBefore) {
        return update()
                .set("last_alert_time", alertTime)
                .eq("id", messageId)
                .eq("status", MqMessageStatus.NEED_MANUAL.name())
                .and(wrapper -> wrapper.isNull("last_alert_time").or().le("last_alert_time", suppressBefore))
                .update();
    }

    @Override
    public boolean markRetrying(Long messageId,
                                Collection<MqMessageStatus> fromStatuses,
                                Integer expectedRetryCount,
                                LocalDateTime nextRetryTime,
                                String reason) {
        return baseMapper.markRetrying(messageId, toCodes(fromStatuses), expectedRetryCount, nextRetryTime, reason) > 0;
    }

    @Override
    public boolean scheduleNextRetry(Long messageId,
                                     Collection<MqMessageStatus> fromStatuses,
                                     LocalDateTime nextRetryTime,
                                     String reason) {
        return baseMapper.scheduleNextRetry(messageId, toCodes(fromStatuses), nextRetryTime, reason) > 0;
    }

    private boolean updateStatusByBizIfIn(String bizType,
                                          Long bizId,
                                          Collection<MqMessageStatus> fromStatuses,
                                          MqMessageStatus toStatus,
                                          String reason,
                                          LocalDateTime nextRetryTime) {
        return baseMapper.updateStatusByBizIfIn(bizType, bizId, toCodes(fromStatuses), toStatus.name(), reason, nextRetryTime) > 0;
    }

    private Collection<String> toCodes(Collection<MqMessageStatus> statuses) {
        return statuses.stream().map(Enum::name).collect(Collectors.toList());
    }

    public static String seckillOrderBizType() {
        return SECKILL_ORDER_BIZ_TYPE;
    }
}
