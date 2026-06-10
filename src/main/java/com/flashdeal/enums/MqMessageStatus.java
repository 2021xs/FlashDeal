package com.flashdeal.enums;

public enum MqMessageStatus {

    INIT,
    SENT,
    RETRYING,
    CONFIRMED,
    CONFIRM_FAILED,
    RETURNED,
    CONSUMED,
    FAILED,
    NEED_MANUAL
}
