package com.flashdeal.dto;

public enum OrderProcessStatus {
    SUCCESS,
    IDEMPOTENT_SUCCESS,
    RETRYABLE_FAILED,
    NON_RETRYABLE_FAILED
}
