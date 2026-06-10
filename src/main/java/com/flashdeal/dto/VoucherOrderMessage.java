package com.flashdeal.dto;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class VoucherOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long messageId;

    private Long orderId;

    private Long userId;

    private Long voucherId;

    private LocalDateTime createTime;
}
