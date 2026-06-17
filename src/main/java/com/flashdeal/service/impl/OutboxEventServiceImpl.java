package com.flashdeal.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.OrderTimeoutMessage;
import com.flashdeal.dto.RedisStockRecoveryMessage;
import com.flashdeal.entity.OutboxEvent;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.enums.OutboxEventStatus;
import com.flashdeal.mapper.OutboxEventMapper;
import com.flashdeal.service.IOutboxEventService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.flashdeal.utils.RabbitConstants.ORDER_TIMEOUT_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.ORDER_TIMEOUT_ROUTING_KEY;

@Service
public class OutboxEventServiceImpl extends ServiceImpl<OutboxEventMapper, OutboxEvent>
        implements IOutboxEventService {

    @Resource
    private ObjectMapper objectMapper;

    @Value("${order.timeout.seconds:900}")
    private long orderTimeoutSeconds;

    @Value("${outbox.order-timeout.max-retry-count:5}")
    private int maxRetryCount;

    @Value("${outbox.redis-stock-recovery.max-retry-count:10}")
    private int redisStockRecoveryMaxRetryCount;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveOrderTimeoutEvents(Collection<VoucherOrder> orders) {
        if (orders == null || orders.isEmpty()) {
            return;
        }
        List<OutboxEvent> events = orders.stream().map(this::buildOrderTimeoutEvent).collect(Collectors.toList());
        if (!saveBatch(events)) {
            throw new IllegalStateException("Save order timeout outbox events failed");
        }
    }

    @Override
    public void saveRedisStockRecoveryEvent(VoucherOrder order) {
        if (!save(buildRedisStockRecoveryEvent(order))) {
            throw new IllegalStateException("Save Redis stock recovery outbox event failed, orderId=" + order.getId());
        }
    }

    @Override
    public List<OutboxEvent> listPublishable(LocalDateTime now, int limit) {
        return baseMapper.selectPublishable(now, Math.max(1, limit));
    }

    @Override
    public boolean claimSending(OutboxEvent event) {
        return baseMapper.claimSending(event.getId(), event.getStatus(), event.getRetryCount()) > 0;
    }

    @Override
    public boolean markSent(OutboxEvent event) {
        return baseMapper.markSent(event.getId(), event.getRetryCount()) > 0;
    }

    @Override
    public boolean markPublishFailed(OutboxEvent event, LocalDateTime nextRetryTime, String reason) {
        return baseMapper.markPublishFailed(
                event.getId(), event.getRetryCount(), nextRetryTime, limitReason(reason)) > 0;
    }

    @Override
    public int recoverStuckSending(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit) {
        return baseMapper.recoverStuckSending(staleBefore, nextRetryTime, Math.max(1, limit));
    }

    @Override
    public List<OutboxEvent> listNeedManual(int limit) {
        return baseMapper.selectNeedManual(Math.max(1, limit));
    }

    @Override
    public List<OutboxEvent> listNeedManualForAlert(String eventType, LocalDateTime suppressBefore, int limit) {
        return query()
                .eq("event_type", eventType)
                .eq("status", OutboxEventStatus.NEED_MANUAL.name())
                .and(wrapper -> wrapper.isNull("last_alert_time").or().le("last_alert_time", suppressBefore))
                .orderByAsc("updated_time")
                .last("LIMIT " + Math.max(1, limit))
                .list();
    }

    @Override
    public boolean markNeedManualAlerted(Long id, LocalDateTime alertTime, LocalDateTime suppressBefore) {
        return update()
                .set("last_alert_time", alertTime)
                .eq("id", id)
                .eq("status", OutboxEventStatus.NEED_MANUAL.name())
                .and(wrapper -> wrapper.isNull("last_alert_time").or().le("last_alert_time", suppressBefore))
                .update();
    }

    @Override
    public List<OutboxEvent> listRedisStockRecoveryPublishable(LocalDateTime now, int limit) {
        return baseMapper.selectRedisStockRecoveryPublishable(now, Math.max(1, limit));
    }

    @Override
    public int recoverStuckRedisStockRecovery(LocalDateTime staleBefore, LocalDateTime nextRetryTime, int limit) {
        return baseMapper.recoverStuckRedisStockRecovery(staleBefore, nextRetryTime, Math.max(1, limit));
    }

    @Override
    public List<OutboxEvent> listRedisStockRecoveryNeedManual(int limit) {
        return baseMapper.selectRedisStockRecoveryNeedManual(Math.max(1, limit));
    }

    OutboxEvent buildOrderTimeoutEvent(VoucherOrder order) {
        LocalDateTime createTime = order.getCreateTime() == null ? LocalDateTime.now() : order.getCreateTime();
        LocalDateTime expireTime = createTime.plusSeconds(Math.max(1L, orderTimeoutSeconds));
        OrderTimeoutMessage message = new OrderTimeoutMessage();
        message.setOrderId(order.getId());
        message.setUserId(order.getUserId());
        message.setVoucherId(order.getVoucherId());
        message.setCreateTime(createTime);
        message.setExpireTime(expireTime);
        return new OutboxEvent()
                .setEventId(UUID.randomUUID().toString().replace("-", ""))
                .setEventType(ORDER_TIMEOUT_EVENT)
                .setBizKey("order:" + order.getId())
                .setExchangeName(ORDER_TIMEOUT_EXCHANGE)
                .setRoutingKey(ORDER_TIMEOUT_ROUTING_KEY)
                .setPayload(toJson(message))
                .setStatus(OutboxEventStatus.INIT.name())
                .setRetryCount(0)
                .setMaxRetryCount(Math.max(1, maxRetryCount))
                .setNextRetryTime(LocalDateTime.now())
                .setExpireTime(expireTime);
    }

    OutboxEvent buildRedisStockRecoveryEvent(VoucherOrder order) {
        RedisStockRecoveryMessage message = new RedisStockRecoveryMessage();
        message.setOrderId(order.getId());
        message.setUserId(order.getUserId());
        message.setVoucherId(order.getVoucherId());
        return new OutboxEvent()
                .setEventId(UUID.randomUUID().toString().replace("-", ""))
                .setEventType(REDIS_STOCK_RECOVERY_EVENT)
                .setBizKey("order:" + order.getId())
                .setExchangeName("INTERNAL")
                .setRoutingKey("REDIS_STOCK_RECOVERY")
                .setPayload(toJson(message))
                .setStatus(OutboxEventStatus.INIT.name())
                .setRetryCount(0)
                .setMaxRetryCount(Math.max(1, redisStockRecoveryMaxRetryCount))
                .setNextRetryTime(LocalDateTime.now());
    }

    private String toJson(OrderTimeoutMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialize order timeout outbox payload failed, orderId="
                    + message.getOrderId(), e);
        }
    }

    private String toJson(RedisStockRecoveryMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialize Redis stock recovery payload failed, orderId="
                    + message.getOrderId(), e);
        }
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 512 ? reason : reason.substring(0, 512);
    }
}
