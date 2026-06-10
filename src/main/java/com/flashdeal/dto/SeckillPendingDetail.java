package com.flashdeal.dto;

import lombok.Data;

@Data
public class SeckillPendingDetail {

    private Long voucherId;
    private Long userId;
    private Long orderId;
    private Long messageId;
    private Long createTime;
}
