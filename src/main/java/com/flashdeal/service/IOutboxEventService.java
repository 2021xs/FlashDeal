package com.flashdeal.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.entity.VoucherOrder;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface IOutboxEventService extends IService<OutboxEvent> {

    String ORDER_TIMEOUT_EVENT = "ORDER_TIMEOUT";

    void saveOrderTimeoutEvents(Collection<VoucherOrder> orders);

    List<OutboxEvent> listPublishable(LocalDateTime now, int limit);

    boolean claimSending(OutboxEvent event);

    boolean markSent(OutboxEvent event);

    boolean markPublishFailed(OutboxEvent event, LocalDateTime nextRetryTime, String reason);

    int recoverStuckSending(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit);

    List<OutboxEvent> listNeedManual(int limit);
}
