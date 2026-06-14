package com.flashdeal.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.dto.OrderProcessStatus;
import com.flashdeal.dto.Result;
import com.flashdeal.dto.SeckillOrderResultDTO;
import com.flashdeal.dto.UserDTO;
import com.flashdeal.dto.VoucherOrderMessage;
import com.flashdeal.entity.MqMessage;
import com.flashdeal.entity.SeckillVoucher;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.enums.MqMessageStatus;
import com.flashdeal.enums.VoucherOrderStatus;
import com.flashdeal.mapper.SeckillVoucherMapper;
import com.flashdeal.mapper.VoucherOrderMapper;
import com.flashdeal.mq.VoucherOrderProducer;
import com.flashdeal.service.IMqMessageService;
import com.flashdeal.service.IOutboxEventService;
import com.flashdeal.service.ISeckillVoucherService;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.service.SeckillReservationService;
import com.flashdeal.utils.RedisIdWorker;
import com.flashdeal.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_ROUTING_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_ORDER_LOCK_KEY;
import static com.flashdeal.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private VoucherOrderProducer voucherOrderProducer;

    @Resource
    private TransactionTemplate transactionTemplate;

    @Resource
    private IMqMessageService mqMessageService;

    @Resource
    private IOutboxEventService outboxEventService;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private SeckillReservationService seckillReservationService;

    @Value("${seckill.mq-message.max-retry-count:3}")
    private Integer mqMessageMaxRetryCount;

    @Value("${seckill.mq-message.initial-next-retry-delay-seconds:30}")
    private Long mqMessageInitialNextRetryDelaySeconds;

    @Value("${seckill.batch.fallback-enabled:true}")
    private boolean batchFallbackEnabled;

    @Value("${seckill.batch.fallback-limit-per-voucher:20}")
    private int fallbackLimitPerVoucher;

    @Value("${seckill.batch.fallback-limit-per-batch:50}")
    private int fallbackLimitPerBatch;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        Result checkResult = checkSeckillVoucherAvailable(seckillVoucher);
        if (checkResult != null) {
            return checkResult;
        }

        long orderId = redisIdWorker.nextId("order");
        long messageId = redisIdWorker.nextId("mq");
        long nowMillis = System.currentTimeMillis();

        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId),
                String.valueOf(messageId), String.valueOf(nowMillis)
        );
        int r = result == null ? -1 : result.intValue();
        if (r != 0) {
            return Result.fail(buildSeckillFailMessage(r));
        }

        VoucherOrderMessage message = buildVoucherOrderMessage(voucherId, userId, orderId, messageId);
        try {
            saveMqMessageBeforeSend(message);
        } catch (Exception e) {
            log.error("Save seckill mq message failed after Lua success; keep Redis pending and wait for Redis-MySQL reconcile, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId, e);
            return Result.fail("系统繁忙，请稍后重试");
        }
        try {
            voucherOrderProducer.sendSeckillOrder(message);
        } catch (Exception e) {
            log.error("Send seckill order message failed after reliable message saved; keep Redis pending and wait for MQ retry, messageId={}, orderId={}, userId={}, voucherId={}",
                    messageId, orderId, userId, voucherId, e);
            return Result.ok(orderId);
        }
        return Result.ok(orderId);
    }

    @Override
    public Result querySeckillResult(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单ID不能为空");
        }
        UserDTO currentUser = UserHolder.getUser();
        if (currentUser == null || currentUser.getId() == null) {
            return Result.fail("用户未登录");
        }
        Long userId = currentUser.getId();
        VoucherOrder order = getById(orderId);
        if (order != null) {
            if (!Objects.equals(order.getUserId(), userId)) {
                return Result.fail("无权查询该订单");
            }
            return Result.ok(buildSeckillResult(orderId, "SUCCESS", order.getStatus(), null, "订单已生成，请继续完成支付"));
        }

        MqMessage mqMessage = mqMessageService.query()
                .eq("biz_type", IMqMessageService.SECKILL_ORDER_BIZ_TYPE)
                .eq("biz_id", orderId)
                .one();
        if (mqMessage == null) {
            return Result.ok(buildSeckillResult(orderId, "UNKNOWN", null, null, "订单结果不存在，请稍后刷新"));
        }
        Long messageUserId = parseMessageUserId(mqMessage);
        if (messageUserId == null) {
            return Result.ok(buildSeckillResult(orderId, "UNKNOWN", null, null, "订单状态暂时无法确认，请稍后刷新"));
        }
        if (!Objects.equals(messageUserId, userId)) {
            return Result.fail("无权查询该订单");
        }

        MqMessageStatus status = parseMqMessageStatus(mqMessage.getStatus());
        if (status == null) {
            return Result.ok(buildSeckillResult(orderId, "UNKNOWN", null, mqMessage.getStatus(), "订单状态异常，请稍后刷新"));
        }
        if (status == MqMessageStatus.FAILED) {
            return Result.ok(buildSeckillResult(orderId, "FAILED", null, mqMessage.getStatus(), "下单失败"));
        }
        if (status == MqMessageStatus.NEED_MANUAL) {
            return Result.ok(buildSeckillResult(orderId, "NEED_MANUAL", null, mqMessage.getStatus(), "订单异常处理中"));
        }
        if (status == MqMessageStatus.CONSUMED) {
            return Result.ok(buildSeckillResult(orderId, "UNKNOWN", null, mqMessage.getStatus(), "订单状态异常，请稍后刷新"));
        }
        return Result.ok(buildSeckillResult(orderId, "PROCESSING", null, mqMessage.getStatus(), "排队中"));
    }

    private MqMessageStatus parseMqMessageStatus(String status) {
        try {
            return status == null ? null : MqMessageStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private SeckillOrderResultDTO buildSeckillResult(Long orderId, String result, Integer orderStatus,
                                                     String mqStatus, String message) {
        SeckillOrderResultDTO dto = new SeckillOrderResultDTO();
        dto.setOrderId(orderId);
        dto.setResult(result);
        dto.setOrderStatus(orderStatus);
        dto.setMqStatus(mqStatus);
        dto.setMessage(message);
        return dto;
    }

    private Long parseMessageUserId(MqMessage mqMessage) {
        try {
            VoucherOrderMessage orderMessage = objectMapper.readValue(mqMessage.getMessageBody(), VoucherOrderMessage.class);
            return orderMessage == null ? null : orderMessage.getUserId();
        } catch (Exception e) {
            log.warn("Parse seckill mq message body failed when query result, messageId={}, bizId={}",
                    mqMessage.getId(), mqMessage.getBizId(), e);
            return null;
        }
    }

    private Result checkSeckillVoucherAvailable(SeckillVoucher seckillVoucher) {
        if (seckillVoucher == null) {
            return Result.fail("秒杀券不存在");
        }
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        if (beginTime != null && now.isBefore(beginTime)) {
            return Result.fail("秒杀尚未开始");
        }
        LocalDateTime endTime = seckillVoucher.getEndTime();
        if (endTime != null && now.isAfter(endTime)) {
            return Result.fail("秒杀已结束");
        }
        Integer stock = seckillVoucher.getStock();
        if (stock != null && stock < 0) {
            return Result.fail("秒杀活动不可用");
        }
        return null;
    }

    private String buildSeckillFailMessage(int code) {
        if (code == 1) {
            return "库存不足";
        }
        if (code == 2) {
            return "不能重复下单";
        }
        if (code == 3) {
            return "秒杀库存未初始化或活动不可用";
        }
        return "系统繁忙，请稍后重试";
    }

    @Transactional
    void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        RLock redisLock = redissonClient.getLock(SECKILL_ORDER_LOCK_KEY + voucherId + ":" + userId);
        boolean isLock = redisLock.tryLock();
        if (!isLock) {
            throw new IllegalStateException("Seckill order is being processed, userId=" + userId + ", voucherId=" + voucherId);
        }

        try {
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                log.warn("Duplicate seckill order ignored, userId={}, voucherId={}", userId, voucherId);
                return;
            }

            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!success) {
                throw new IllegalStateException("Seckill voucher stock is not enough, voucherId=" + voucherId);
            }

            try {
                voucherOrder.setStatus(VoucherOrderStatus.UNPAID.getCode());
                voucherOrder.setCreateTime(LocalDateTime.now());
                boolean orderSaved = save(voucherOrder);
                if (!orderSaved) {
                    throw new IllegalStateException("Save seckill voucher order failed, orderId="
                            + voucherOrder.getId() + ", userId=" + userId + ", voucherId=" + voucherId);
                }
                outboxEventService.saveOrderTimeoutEvents(Collections.singletonList(voucherOrder));
            } catch (DuplicateKeyException e) {
                log.warn("Duplicate seckill order ignored by unique key, userId={}, voucherId={}", userId, voucherId);
                throw e;
            }
        } finally {
            redisLock.unlock();
        }
    }

    @Override
    public BatchVoucherOrderResult createVoucherOrdersBatch(List<VoucherOrder> voucherOrders) {
        return createVoucherOrdersBatch(voucherOrders, Collections.emptyMap());
    }

    @Override
    public BatchVoucherOrderResult createClaimedVoucherOrdersBatch(List<VoucherOrderMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return new BatchVoucherOrderResult();
        }
        Map<Long, Long> messageIdsByOrderId = messages.stream()
                .filter(message -> message.getOrderId() != null && message.getMessageId() != null)
                .collect(Collectors.toMap(VoucherOrderMessage::getOrderId, VoucherOrderMessage::getMessageId, (left, right) -> left));
        List<VoucherOrder> orders = messages.stream()
                .map(message -> {
                    VoucherOrder order = new VoucherOrder();
                    order.setId(message.getOrderId());
                    order.setUserId(message.getUserId());
                    order.setVoucherId(message.getVoucherId());
                    return order;
                })
                .collect(Collectors.toList());
        return createVoucherOrdersBatch(orders, messageIdsByOrderId);
    }

    @Override
    public boolean hasExistingOrder(Long orderId, Long userId, Long voucherId) {
        if (orderId != null && getById(orderId) != null) {
            return true;
        }
        return query().eq("user_id", userId).eq("voucher_id", voucherId).count() > 0;
    }

    private BatchVoucherOrderResult createVoucherOrdersBatch(List<VoucherOrder> voucherOrders,
                                                              Map<Long, Long> messageIdsByOrderId) {
        BatchVoucherOrderResult result = new BatchVoucherOrderResult();
        if (voucherOrders == null || voucherOrders.isEmpty()) {
            return result;
        }

        Set<String> seenUserVoucherKeys = new HashSet<>();
        List<VoucherOrder> deduplicatedOrders = new ArrayList<>();
        Map<String, VoucherOrder> primaryOrdersByUserVoucherKey = new HashMap<>();
        Map<String, List<VoucherOrder>> duplicateOrdersByUserVoucherKey = new HashMap<>();
        for (VoucherOrder order : voucherOrders) {
            String key = buildUserVoucherKey(order.getUserId(), order.getVoucherId());
            if (seenUserVoucherKeys.add(key)) {
                deduplicatedOrders.add(order);
                primaryOrdersByUserVoucherKey.put(key, order);
            } else {
                duplicateOrdersByUserVoucherKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(order);
                log.warn("Duplicate seckill order message deferred inside batch until primary result is known, orderId={}, userId={}, voucherId={}",
                        order.getId(), order.getUserId(), order.getVoucherId());
            }
        }

        List<VoucherOrder> existingOrders = baseMapper.selectExistingUserVoucherPairs(deduplicatedOrders);
        Set<String> existingUserVoucherKeys = new HashSet<>();
        for (VoucherOrder existingOrder : existingOrders) {
            existingUserVoucherKeys.add(buildUserVoucherKey(existingOrder.getUserId(), existingOrder.getVoucherId()));
        }

        List<VoucherOrder> ordersToCreate = new ArrayList<>();
        List<VoucherOrder> idempotentOrders = new ArrayList<>();
        for (VoucherOrder order : deduplicatedOrders) {
            if (existingUserVoucherKeys.contains(buildUserVoucherKey(order.getUserId(), order.getVoucherId()))) {
                result.addIdempotentOrderId(order.getId());
                idempotentOrders.add(order);
                log.warn("Duplicate seckill order message ignored before batch insert, orderId={}, userId={}, voucherId={}",
                        order.getId(), order.getUserId(), order.getVoucherId());
            } else {
                order.setStatus(VoucherOrderStatus.UNPAID.getCode());
                if (order.getPayType() == null) {
                    order.setPayType(1);
                }
                ordersToCreate.add(order);
            }
        }

        if (!idempotentOrders.isEmpty() && !messageIdsByOrderId.isEmpty()) {
            transactionTemplate.executeWithoutResult(
                    status -> markMessagesConsumedInTransaction(idempotentOrders, messageIdsByOrderId));
        }

        if (ordersToCreate.isEmpty()) {
            propagateDuplicateOrderResults(primaryOrdersByUserVoucherKey, duplicateOrdersByUserVoucherKey, result);
            markConfirmedDuplicateMessagesConsumed(duplicateOrdersByUserVoucherKey, result, messageIdsByOrderId);
            return result;
        }

        Map<Long, List<VoucherOrder>> ordersByVoucherId = ordersToCreate.stream()
                .collect(Collectors.groupingBy(VoucherOrder::getVoucherId));
        FallbackBudget fallbackBudget = new FallbackBudget();
        for (Map.Entry<Long, List<VoucherOrder>> entry : ordersByVoucherId.entrySet()) {
            createVoucherOrderGroup(entry.getKey(), entry.getValue(), result, messageIdsByOrderId, fallbackBudget);
        }
        propagateDuplicateOrderResults(primaryOrdersByUserVoucherKey, duplicateOrdersByUserVoucherKey, result);
        markConfirmedDuplicateMessagesConsumed(duplicateOrdersByUserVoucherKey, result, messageIdsByOrderId);
        return result;
    }

    private void propagateDuplicateOrderResults(Map<String, VoucherOrder> primaryOrdersByUserVoucherKey,
                                                Map<String, List<VoucherOrder>> duplicateOrdersByUserVoucherKey,
                                                BatchVoucherOrderResult result) {
        if (duplicateOrdersByUserVoucherKey.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<VoucherOrder>> entry : duplicateOrdersByUserVoucherKey.entrySet()) {
            VoucherOrder primaryOrder = primaryOrdersByUserVoucherKey.get(entry.getKey());
            OrderProcessStatus primaryStatus = primaryOrder == null ? null : result.getStatus(primaryOrder.getId());
            for (VoucherOrder duplicateOrder : entry.getValue()) {
                propagateDuplicateOrderResult(primaryOrder, duplicateOrder, primaryStatus, result);
            }
        }
    }

    private void propagateDuplicateOrderResult(VoucherOrder primaryOrder,
                                               VoucherOrder duplicateOrder,
                                               OrderProcessStatus primaryStatus,
                                               BatchVoucherOrderResult result) {
        Long duplicateOrderId = duplicateOrder.getId();
        if (primaryStatus == OrderProcessStatus.SUCCESS
                || primaryStatus == OrderProcessStatus.IDEMPOTENT_SUCCESS) {
            result.addIdempotentOrderId(duplicateOrderId);
            log.warn("Duplicate seckill order message marked idempotent after primary order confirmed, primaryOrderId={}, duplicateOrderId={}, userId={}, voucherId={}, primaryStatus={}",
                    primaryOrder == null ? null : primaryOrder.getId(), duplicateOrderId,
                    duplicateOrder.getUserId(), duplicateOrder.getVoucherId(), primaryStatus);
            return;
        }
        String reason = "PRIMARY_ORDER_" + (primaryStatus == null ? "UNKNOWN" : primaryStatus.name());
        if (primaryStatus == OrderProcessStatus.NON_RETRYABLE_FAILED) {
            result.addNonRetryableFailedOrderId(duplicateOrderId, reason);
        } else {
            result.addRetryableFailedOrderId(duplicateOrderId, reason);
        }
        log.warn("Duplicate seckill order message follows primary failure, primaryOrderId={}, duplicateOrderId={}, userId={}, voucherId={}, primaryStatus={}, action={}",
                primaryOrder == null ? null : primaryOrder.getId(), duplicateOrderId,
                duplicateOrder.getUserId(), duplicateOrder.getVoucherId(), primaryStatus,
                primaryStatus == OrderProcessStatus.NON_RETRYABLE_FAILED ? "nonRetryableFailed" : "retryableFailed");
    }

    private void markConfirmedDuplicateMessagesConsumed(Map<String, List<VoucherOrder>> duplicateOrdersByUserVoucherKey,
                                                        BatchVoucherOrderResult result,
                                                        Map<Long, Long> messageIdsByOrderId) {
        if (duplicateOrdersByUserVoucherKey.isEmpty() || messageIdsByOrderId.isEmpty()) {
            return;
        }
        List<VoucherOrder> confirmedDuplicates = duplicateOrdersByUserVoucherKey.values().stream()
                .flatMap(List::stream)
                .filter(order -> result.getStatus(order.getId()) == OrderProcessStatus.IDEMPOTENT_SUCCESS)
                .collect(Collectors.toList());
        if (!confirmedDuplicates.isEmpty()) {
            transactionTemplate.executeWithoutResult(
                    status -> markMessagesConsumedInTransaction(confirmedDuplicates, messageIdsByOrderId));
        }
    }

    private void createVoucherOrderGroup(Long voucherId,
                                         List<VoucherOrder> orders,
                                         BatchVoucherOrderResult result,
                                         Map<Long, Long> messageIdsByOrderId,
                                         FallbackBudget fallbackBudget) {
        try {
            transactionTemplate.executeWithoutResult(
                    status -> createVoucherOrderGroupInTransaction(voucherId, orders, messageIdsByOrderId));
            for (VoucherOrder order : orders) {
                result.addSuccessOrderId(order.getId());
            }
        } catch (BatchStockNotEnoughException e) {
            fallbackCreateOrdersOneByOne(voucherId, orders, result, messageIdsByOrderId, fallbackBudget,
                    "BATCH_STOCK_NOT_ENOUGH");
        } catch (DuplicateKeyException e) {
            handleDuplicateKeyBatchFailure(voucherId, orders, result, messageIdsByOrderId, fallbackBudget, e);
        } catch (Exception e) {
            String reason = summarizeException(e);
            for (VoucherOrder order : orders) {
                result.addRetryableFailedOrderId(order.getId(), reason);
            }
            result.setBatchLevelException(reason);
            log.error("Batch create seckill order group failed with retryable technical error, voucherId={}, size={}",
                    voucherId, orders.size(), e);
        }
    }

    private void createVoucherOrderGroupInTransaction(Long voucherId,
                                                      List<VoucherOrder> ordersToCreate,
                                                      Map<Long, Long> messageIdsByOrderId) {
        int count = ordersToCreate.size();
        LocalDateTime createTime = LocalDateTime.now();
        ordersToCreate.forEach(order -> order.setCreateTime(createTime));
        int updated = seckillVoucherMapper.decrementStockBatch(voucherId, count);
        if (updated != 1) {
            throw new BatchStockNotEnoughException("Batch decrement seckill voucher stock failed, voucherId="
                    + voucherId + ", count=" + count);
        }
        int inserted = baseMapper.insertBatch(ordersToCreate);
        if (inserted != count) {
            throw new IllegalStateException("Batch insert seckill voucher orders failed, inserted="
                    + inserted + ", expected=" + count);
        }
        outboxEventService.saveOrderTimeoutEvents(ordersToCreate);
        markMessagesConsumedInTransaction(ordersToCreate, messageIdsByOrderId);
    }

    private void handleDuplicateKeyBatchFailure(Long voucherId,
                                                List<VoucherOrder> orders,
                                                BatchVoucherOrderResult result,
                                                Map<Long, Long> messageIdsByOrderId,
                                                FallbackBudget fallbackBudget,
                                                DuplicateKeyException cause) {
        List<VoucherOrder> unresolvedOrders = new ArrayList<>(orders);
        try {
            List<VoucherOrder> existingOrders = baseMapper.selectExistingUserVoucherPairs(orders);
            Set<String> existingUserVoucherKeys = existingOrders.stream()
                    .map(order -> buildUserVoucherKey(order.getUserId(), order.getVoucherId()))
                    .collect(Collectors.toSet());
            List<VoucherOrder> confirmedExistingOrders = unresolvedOrders.stream()
                    .filter(order -> existingUserVoucherKeys.contains(buildUserVoucherKey(order.getUserId(), order.getVoucherId())))
                    .collect(Collectors.toList());
            if (!confirmedExistingOrders.isEmpty()) {
                transactionTemplate.executeWithoutResult(
                        status -> markMessagesConsumedInTransaction(confirmedExistingOrders, messageIdsByOrderId));
                for (VoucherOrder order : confirmedExistingOrders) {
                    result.addIdempotentOrderId(order.getId());
                    log.warn("Batch duplicate key confirmed existing order by requery, orderId={}, userId={}, voucherId={}, action=idempotentSuccess",
                            order.getId(), order.getUserId(), order.getVoucherId());
                }
                unresolvedOrders = unresolvedOrders.stream()
                        .filter(order -> !existingUserVoucherKeys.contains(buildUserVoucherKey(order.getUserId(), order.getVoucherId())))
                        .collect(Collectors.toList());
            }
            log.warn("Batch insert duplicate key handled by requery before limited fallback, voucherId={}, batchSize={}, confirmedExistingCount={}, unresolvedCount={}, fallbackEnabled={}, fallbackLimitPerVoucher={}, fallbackLimitPerBatch={}, failureType={}, action=requery_then_limited_fallback",
                    voucherId, orders.size(), confirmedExistingOrders.size(), unresolvedOrders.size(),
                    batchFallbackEnabled, normalizedFallbackLimitPerVoucher(), normalizedFallbackLimitPerBatch(),
                    "DUPLICATE_KEY");
        } catch (Exception e) {
            log.error("Batch duplicate key existing-order requery failed before limited fallback, voucherId={}, batchSize={}, fallbackEnabled={}, fallbackLimitPerVoucher={}, fallbackLimitPerBatch={}, failureType={}, action=limited_fallback_after_requery_failed",
                    voucherId, orders.size(), batchFallbackEnabled, normalizedFallbackLimitPerVoucher(),
                    normalizedFallbackLimitPerBatch(), "DUPLICATE_KEY_REQUERY_FAILED", e);
            unresolvedOrders = new ArrayList<>(orders);
        }
        if (!unresolvedOrders.isEmpty()) {
            fallbackCreateOrdersOneByOne(voucherId, unresolvedOrders, result, messageIdsByOrderId, fallbackBudget,
                    "DUPLICATE_KEY", cause);
        }
    }

    private void fallbackCreateOrdersOneByOne(Long voucherId,
                                               List<VoucherOrder> ordersToCreate,
                                               BatchVoucherOrderResult result,
                                               Map<Long, Long> messageIdsByOrderId,
                                               FallbackBudget fallbackBudget,
                                               String failureType) {
        fallbackCreateOrdersOneByOne(voucherId, ordersToCreate, result, messageIdsByOrderId, fallbackBudget,
                failureType, null);
    }

    private void fallbackCreateOrdersOneByOne(Long voucherId,
                                               List<VoucherOrder> ordersToCreate,
                                               BatchVoucherOrderResult result,
                                               Map<Long, Long> messageIdsByOrderId,
                                               FallbackBudget fallbackBudget,
                                               String failureType,
                                               Exception cause) {
        int allowedFallbackCount = calculateAllowedFallbackCount(fallbackBudget);
        int actualFallbackCount = Math.min(ordersToCreate.size(), allowedFallbackCount);
        int skippedFallbackCount = Math.max(0, ordersToCreate.size() - actualFallbackCount);
        if (cause == null) {
            log.warn("Batch create seckill orders entering limited fallback, voucherId={}, batchSize={}, fallbackEnabled={}, fallbackLimitPerVoucher={}, fallbackLimitPerBatch={}, actualFallbackCount={}, skippedFallbackCount={}, failureType={}, action=limited_single_fallback",
                    voucherId, ordersToCreate.size(), batchFallbackEnabled, normalizedFallbackLimitPerVoucher(),
                    normalizedFallbackLimitPerBatch(), actualFallbackCount, skippedFallbackCount, failureType);
        } else {
            log.warn("Batch create seckill orders entering limited fallback, voucherId={}, batchSize={}, fallbackEnabled={}, fallbackLimitPerVoucher={}, fallbackLimitPerBatch={}, actualFallbackCount={}, skippedFallbackCount={}, failureType={}, action=limited_single_fallback",
                    voucherId, ordersToCreate.size(), batchFallbackEnabled, normalizedFallbackLimitPerVoucher(),
                    normalizedFallbackLimitPerBatch(), actualFallbackCount, skippedFallbackCount, failureType, cause);
        }

        for (int i = 0; i < actualFallbackCount; i++) {
            VoucherOrder order = ordersToCreate.get(i);
            fallbackBudget.used++;
            try {
                transactionTemplate.executeWithoutResult(status -> {
                    createVoucherOrder(order);
                    markMessagesConsumedInTransaction(Collections.singletonList(order), messageIdsByOrderId);
                });
                result.addSuccessOrderId(order.getId());
            } catch (DuplicateKeyException e) {
                transactionTemplate.executeWithoutResult(
                        status -> markMessagesConsumedInTransaction(Collections.singletonList(order), messageIdsByOrderId));
                result.addIdempotentOrderId(order.getId());
                log.warn("Duplicate seckill order ignored in fallback, orderId={}, userId={}, voucherId={}",
                        order.getId(), order.getUserId(), order.getVoucherId());
            } catch (Exception e) {
                if (isNonRetryableBatchFailure(e)) {
                    result.addNonRetryableFailedOrderId(order.getId(), summarizeException(e));
                    log.error("Fallback create seckill order failed with non-retryable error, orderId={}, userId={}, voucherId={}",
                            order.getId(), order.getUserId(), order.getVoucherId(), e);
                } else {
                    result.addRetryableFailedOrderId(order.getId(), summarizeException(e));
                    log.error("Fallback create seckill order failed, orderId={}, userId={}, voucherId={}",
                            order.getId(), order.getUserId(), order.getVoucherId(), e);
                }
            }
        }
        if (skippedFallbackCount > 0) {
            String reason = "BATCH_FALLBACK_LIMIT_EXCEEDED:" + failureType;
            for (int i = actualFallbackCount; i < ordersToCreate.size(); i++) {
                VoucherOrder order = ordersToCreate.get(i);
                result.addRetryableFailedOrderId(order.getId(), reason);
            }
            log.warn("Batch create seckill orders skipped single fallback after limit reached, voucherId={}, batchSize={}, fallbackEnabled={}, fallbackLimitPerVoucher={}, fallbackLimitPerBatch={}, actualFallbackCount={}, skippedFallbackCount={}, failureType={}, action=mark_retryable_failed",
                    voucherId, ordersToCreate.size(), batchFallbackEnabled, normalizedFallbackLimitPerVoucher(),
                    normalizedFallbackLimitPerBatch(), actualFallbackCount, skippedFallbackCount, failureType);
        }
    }

    private int calculateAllowedFallbackCount(FallbackBudget fallbackBudget) {
        if (!batchFallbackEnabled) {
            return 0;
        }
        int perVoucherLimit = normalizedFallbackLimitPerVoucher();
        int batchRemaining = Math.max(0, normalizedFallbackLimitPerBatch() - fallbackBudget.used);
        return Math.min(perVoucherLimit, batchRemaining);
    }

    private int normalizedFallbackLimitPerVoucher() {
        return Math.max(0, fallbackLimitPerVoucher);
    }

    private int normalizedFallbackLimitPerBatch() {
        return Math.max(0, fallbackLimitPerBatch);
    }

    private void markMessagesConsumedInTransaction(List<VoucherOrder> orders,
                                                   Map<Long, Long> messageIdsByOrderId) {
        for (VoucherOrder order : orders) {
            Long messageId = messageIdsByOrderId.get(order.getId());
            if (messageId != null && !mqMessageService.markConsumed(messageId)) {
                throw new IllegalStateException("Mark seckill mq message consumed failed, messageId="
                        + messageId + ", orderId=" + order.getId());
            }
        }
    }

    private boolean isNonRetryableBatchFailure(Exception e) {
        String message = e.getMessage();
        return message != null
                && (message.contains("stock is not enough")
                || message.contains("voucher")
                || message.contains("illegal")
                || message.contains("invalid"));
    }

    private String summarizeException(Throwable e) {
        if (e == null) {
            return null;
        }
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static class BatchStockNotEnoughException extends RuntimeException {
        private BatchStockNotEnoughException(String message) {
            super(message);
        }
    }

    private static class FallbackBudget {
        private int used;
    }

    private String buildUserVoucherKey(Long userId, Long voucherId) {
        return Objects.toString(userId, "") + ":" + Objects.toString(voucherId, "");
    }

    @Override
    @Transactional
    public Result payOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }

        VoucherOrderStatus status = VoucherOrderStatus.fromCode(order.getStatus());
        if (status == VoucherOrderStatus.PAID) {
            return Result.ok("订单已支付");
        }
        if (status == VoucherOrderStatus.CANCELED) {
            return Result.fail("订单已取消，无法支付");
        }
        if (status == VoucherOrderStatus.USED) {
            return Result.fail("订单已核销");
        }
        if (status != VoucherOrderStatus.UNPAID) {
            return Result.fail("订单状态不支持支付");
        }

        boolean success = update()
                .set("status", VoucherOrderStatus.PAID.getCode())
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", VoucherOrderStatus.UNPAID.getCode())
                .update();
        if (success) {
            log.info("Voucher order paid, orderId={}", orderId);
            return Result.ok("支付成功");
        }
        return buildStateChangedResult(orderId, "支付失败，请刷新后重试");
    }

    @Override
    @Transactional
    public Result useOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }

        VoucherOrderStatus status = VoucherOrderStatus.fromCode(order.getStatus());
        if (status == VoucherOrderStatus.USED) {
            return Result.fail("订单已核销，不能重复核销");
        }
        if (status == VoucherOrderStatus.UNPAID) {
            return Result.fail("请先支付");
        }
        if (status == VoucherOrderStatus.CANCELED) {
            return Result.fail("订单已取消，不能核销");
        }
        if (status != VoucherOrderStatus.PAID) {
            return Result.fail("订单状态不支持核销");
        }

        boolean success = update()
                .set("status", VoucherOrderStatus.USED.getCode())
                .set("use_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", VoucherOrderStatus.PAID.getCode())
                .update();
        if (success) {
            log.info("Voucher order used, orderId={}", orderId);
            return Result.ok("核销成功");
        }
        return buildStateChangedResult(orderId, "核销失败，请刷新后重试");
    }

    @Override
    public void closeTimeoutOrder(Long orderId) {
        closeUnpaidOrderIfNecessary(orderId);
    }

    @Override
    public boolean closeUnpaidOrderIfNecessary(Long orderId) {
        VoucherOrder closedOrder = transactionTemplate.execute(status -> closeUnpaidOrderInTransaction(orderId));
        return closedOrder != null;
    }

    private VoucherOrder closeUnpaidOrderInTransaction(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            log.warn("Timeout order not found, orderId={}", orderId);
            return null;
        }

        if (order.getStatus() == null || order.getStatus() != VoucherOrderStatus.UNPAID.getCode()) {
            log.info("Timeout order ignored, orderId={}, status={}", orderId, order.getStatus());
            return null;
        }

        boolean closed = cancelUnpaidOrder(orderId);
        if (!closed) {
            log.info("Timeout order close skipped because status changed, orderId={}", orderId);
            return null;
        }

        Long voucherId = order.getVoucherId();
        boolean stockRecovered = recoverMysqlStock(voucherId);
        if (!stockRecovered) {
            throw new IllegalStateException("Recover MySQL seckill stock failed, orderId=" + orderId + ", voucherId=" + voucherId);
        }
        outboxEventService.saveRedisStockRecoveryEvent(order);

        log.info("Timeout order canceled, MySQL stock recovered and Redis recovery event saved, orderId={}, userId={}, voucherId={}",
                orderId, order.getUserId(), voucherId);
        return order;
    }

    protected boolean cancelUnpaidOrder(Long orderId) {
        return update()
                .set("status", VoucherOrderStatus.CANCELED.getCode())
                .set("cancel_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", VoucherOrderStatus.UNPAID.getCode())
                .update();
    }

    protected boolean recoverMysqlStock(Long voucherId) {
        return seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", voucherId)
                .update();
    }

    private VoucherOrderMessage buildVoucherOrderMessage(Long voucherId, Long userId, long orderId, long messageId) {
        VoucherOrderMessage message = new VoucherOrderMessage();
        message.setMessageId(messageId);
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setVoucherId(voucherId);
        message.setCreateTime(LocalDateTime.now());
        return message;
    }

    private void saveMqMessageBeforeSend(VoucherOrderMessage message) {
        MqMessage mqMessage = new MqMessage();
        mqMessage.setId(message.getMessageId());
        mqMessage.setBizType(IMqMessageService.SECKILL_ORDER_BIZ_TYPE);
        mqMessage.setBizId(message.getOrderId());
        mqMessage.setExchangeName(SECKILL_ORDER_EXCHANGE);
        mqMessage.setRoutingKey(SECKILL_ORDER_ROUTING_KEY);
        mqMessage.setMessageBody(toJson(message));
        mqMessage.setStatus(MqMessageStatus.INIT.name());
        mqMessage.setRetryCount(0);
        mqMessage.setMaxRetryCount(mqMessageMaxRetryCount);
        mqMessage.setNextRetryTime(LocalDateTime.now().plusSeconds(mqMessageInitialNextRetryDelaySeconds));
        if (!mqMessageService.save(mqMessage)) {
            throw new IllegalStateException("Save mq message returned false, messageId=" + message.getMessageId());
        }
    }

    private String toJson(VoucherOrderMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialize voucher order message failed, messageId=" + message.getMessageId(), e);
        }
    }

    private String limitReason(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 512 ? reason : reason.substring(0, 512);
    }

    private Result buildStateChangedResult(Long orderId, String defaultMessage) {
        VoucherOrder latest = getById(orderId);
        if (latest == null) {
            return Result.fail("订单不存在");
        }
        VoucherOrderStatus latestStatus = VoucherOrderStatus.fromCode(latest.getStatus());
        if (latestStatus == VoucherOrderStatus.PAID) {
            return Result.ok("订单已支付");
        }
        if (latestStatus == VoucherOrderStatus.CANCELED) {
            return Result.fail("订单已取消");
        }
        if (latestStatus == VoucherOrderStatus.USED) {
            return Result.fail("订单已核销");
        }
        return Result.fail(defaultMessage);
    }
}
