package com.flashdeal.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.service.IMqMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class MqMessageRetryTask {

    private static final List<MqMessageStatus> CLAIMABLE_STATUSES = Arrays.asList(
            MqMessageStatus.INIT,
            MqMessageStatus.SEND_FAILED,
            MqMessageStatus.CONFIRM_FAILED,
            MqMessageStatus.RETURNED,
            MqMessageStatus.SENT
    );

    @Resource
    private IMqMessageService mqMessageService;

    @Resource
    private VoucherOrderProducer voucherOrderProducer;

    @Resource
    private ObjectMapper objectMapper;

    @Value("${seckill.mq-message.retry.enabled:true}")
    private Boolean retryEnabled;

    @Value("${seckill.mq-message.retry-batch-size:50}")
    private Integer retryBatchSize;

    @Value("${seckill.mq-message.sent-timeout-seconds:300}")
    private Long sentTimeoutSeconds;

    @Value("${seckill.mq-message.initial-next-retry-delay-seconds:30}")
    private Long nextRetryDelaySeconds;

    @Scheduled(fixedDelayString = "#{${seckill.mq-message.retry-interval-seconds:30} * 1000}")
    public void retrySeckillOrderMessages() {
        if (!Boolean.TRUE.equals(retryEnabled)) {
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime sentBefore = now.minusSeconds(sentTimeoutSeconds);
        markExceededRetryMessages(now, sentBefore);
        retryMessages(now, sentBefore);
    }

    private void markExceededRetryMessages(LocalDateTime now, LocalDateTime sentBefore) {
        List<MqMessage> messages = mqMessageService.listExceededRetryMessages(now, sentBefore, retryBatchSize);
        for (MqMessage message : messages) {
            String reason = limitReason("RETRY_EXCEEDED status=" + message.getStatus()
                    + ", retryCount=" + message.getRetryCount());
            boolean updated = mqMessageService.markNeedManualAfterRetryExceeded(
                    message.getId(),
                    MqMessageStatus.valueOf(message.getStatus()),
                    message.getRetryCount(),
                    reason);
            log.error("Seckill mq message retry exceeded, messageId={}, bizId={}, status={}, retryCount={}, updated={}",
                    message.getId(), message.getBizId(), message.getStatus(), message.getRetryCount(), updated);
        }
    }

    private void retryMessages(LocalDateTime now, LocalDateTime sentBefore) {
        List<MqMessage> messages = mqMessageService.listRetryableMessages(now, sentBefore, retryBatchSize);
        for (MqMessage message : messages) {
            boolean retryMarked = mqMessageService.markRetrying(
                    message.getId(),
                    claimFromStatuses(message),
                    message.getRetryCount(),
                    LocalDateTime.now().plusSeconds(nextRetryDelaySeconds),
                    "RETRYING"
            );
            if (!retryMarked) {
                log.warn("Skip seckill mq message resend because retry mark failed, messageId={}, bizId={}, status={}, retryCount={}",
                        message.getId(), message.getBizId(), message.getStatus(), message.getRetryCount());
                continue;
            }
            try {
                VoucherOrderMessage orderMessage = objectMapper.readValue(message.getMessageBody(), VoucherOrderMessage.class);
                orderMessage.setMessageId(message.getId());
                voucherOrderProducer.resendSeckillOrder(orderMessage);
                log.info("Seckill mq message resent, messageId={}, bizId={}, status={}, retryCount={}",
                        message.getId(), message.getBizId(), message.getStatus(), message.getRetryCount() + 1);
            } catch (Exception e) {
                LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(nextRetryDelaySeconds);
                boolean updated = mqMessageService.scheduleNextRetry(
                        message.getId(),
                        Arrays.asList(MqMessageStatus.RETRYING),
                        nextRetryTime,
                        limitReason(e.getMessage())
                );
                log.error("Seckill mq message resend failed, messageId={}, bizId={}, status={}, retryCount={}, updated={}",
                        message.getId(), message.getBizId(), message.getStatus(), message.getRetryCount(), updated, e);
            }
        }
    }

    private List<MqMessageStatus> claimFromStatuses(MqMessage message) {
        if (MqMessageStatus.RETRYING.name().equals(message.getStatus())) {
            return Arrays.asList(MqMessageStatus.RETRYING);
        }
        return CLAIMABLE_STATUSES;
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 512 ? reason : reason.substring(0, 512);
    }
}
