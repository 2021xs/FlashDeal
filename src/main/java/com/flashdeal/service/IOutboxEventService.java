package com.flashdeal.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.entity.VoucherOrder;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface IOutboxEventService extends IService<OutboxEvent> {

    String ORDER_TIMEOUT_EVENT = "ORDER_TIMEOUT";
    String REDIS_STOCK_RECOVERY_EVENT = "REDIS_STOCK_RECOVERY";

    void saveOrderTimeoutEvents(Collection<VoucherOrder> orders);

    void saveRedisStockRecoveryEvent(VoucherOrder order);

    List<OutboxEvent> listPublishable(LocalDateTime now, int limit);

    boolean claimSending(OutboxEvent event);

    boolean markSent(OutboxEvent event);

    boolean markPublishFailed(OutboxEvent event, LocalDateTime nextRetryTime, String reason);

    int recoverStuckSending(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit);

    List<OutboxEvent> listNeedManual(int limit);

    List<OutboxEvent> listRedisStockRecoveryPublishable(LocalDateTime now, int limit);

    int recoverStuckRedisStockRecovery(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit);

    List<OutboxEvent> listRedisStockRecoveryNeedManual(int limit);
}
