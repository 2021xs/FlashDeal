package com.flashdeal.dto;

import lombok.Data;

@Data
public class SeckillReservationState {

    private SeckillReservationStatus status;

    private Long orderId;

    private Long timestamp;

    private String rawValue;

    public boolean isProcessingTimedOut(long nowMillis, long processingTimeoutMillis) {
        return status == SeckillReservationStatus.PROCESSING
                && timestamp != null
                && nowMillis - timestamp >= Math.max(0L, processingTimeoutMillis);
    }
}
