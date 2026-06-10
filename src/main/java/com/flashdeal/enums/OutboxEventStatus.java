package com.flashdeal.enums;

public enum OutboxEventStatus {
    INIT,
    SENDING,
    SENT,
    FAILED,
    NEED_MANUAL
}
