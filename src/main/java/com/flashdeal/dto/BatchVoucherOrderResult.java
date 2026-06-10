package com.flashdeal.dto;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BatchVoucherOrderResult {

    private final Set<Long> successOrderIds = new HashSet<>();
    private final Set<Long> idempotentOrderIds = new HashSet<>();
    private final Set<Long> retryableFailedOrderIds = new HashSet<>();
    private final Set<Long> nonRetryableFailedOrderIds = new HashSet<>();
    private final Map<Long, String> failedReasons = new HashMap<>();
    private final Map<Long, OrderProcessStatus> orderStatusMap = new HashMap<>();
    private String batchLevelException;

    public void addAckOrderId(Long orderId) {
        addSuccessOrderId(orderId);
    }

    public void addFailedOrderId(Long orderId) {
        addRetryableFailedOrderId(orderId, null);
    }

    public void addSuccessOrderId(Long orderId) {
        successOrderIds.add(orderId);
        putStatus(orderId, OrderProcessStatus.SUCCESS);
    }

    public void addIdempotentOrderId(Long orderId) {
        idempotentOrderIds.add(orderId);
        putStatus(orderId, OrderProcessStatus.IDEMPOTENT_SUCCESS);
    }

    public void addRetryableFailedOrderId(Long orderId, String reason) {
        retryableFailedOrderIds.add(orderId);
        putStatus(orderId, OrderProcessStatus.RETRYABLE_FAILED);
        addFailedReason(orderId, reason);
    }

    public void addNonRetryableFailedOrderId(Long orderId, String reason) {
        nonRetryableFailedOrderIds.add(orderId);
        putStatus(orderId, OrderProcessStatus.NON_RETRYABLE_FAILED);
        addFailedReason(orderId, reason);
    }

    private void putStatus(Long orderId, OrderProcessStatus status) {
        if (orderId != null) {
            orderStatusMap.put(orderId, status);
        }
    }

    private void addFailedReason(Long orderId, String reason) {
        if (orderId != null && reason != null) {
            failedReasons.put(orderId, reason);
        }
    }

    public Set<Long> getAckOrderIds() {
        Set<Long> ackOrderIds = new HashSet<>(successOrderIds);
        ackOrderIds.addAll(idempotentOrderIds);
        return Collections.unmodifiableSet(ackOrderIds);
    }

    public Set<Long> getFailedOrderIds() {
        Set<Long> failedOrderIds = new HashSet<>(retryableFailedOrderIds);
        failedOrderIds.addAll(nonRetryableFailedOrderIds);
        return Collections.unmodifiableSet(failedOrderIds);
    }

    public Set<Long> getSuccessOrderIds() {
        return Collections.unmodifiableSet(successOrderIds);
    }

    public Set<Long> getIdempotentOrderIds() {
        return Collections.unmodifiableSet(idempotentOrderIds);
    }

    public Set<Long> getRetryableFailedOrderIds() {
        return Collections.unmodifiableSet(retryableFailedOrderIds);
    }

    public Set<Long> getNonRetryableFailedOrderIds() {
        return Collections.unmodifiableSet(nonRetryableFailedOrderIds);
    }

    public Map<Long, String> getFailedReasons() {
        return Collections.unmodifiableMap(failedReasons);
    }

    public Map<Long, OrderProcessStatus> getOrderStatusMap() {
        return Collections.unmodifiableMap(orderStatusMap);
    }

    public OrderProcessStatus getStatus(Long orderId) {
        return orderStatusMap.get(orderId);
    }

    public boolean hasRetryableFailure() {
        return !retryableFailedOrderIds.isEmpty();
    }

    public boolean hasNonRetryableFailure() {
        return !nonRetryableFailedOrderIds.isEmpty();
    }

    public boolean shouldRetry() {
        return hasRetryableFailure();
    }

    public String getBatchLevelException() {
        return batchLevelException;
    }

    public void setBatchLevelException(String batchLevelException) {
        this.batchLevelException = batchLevelException;
    }
}
