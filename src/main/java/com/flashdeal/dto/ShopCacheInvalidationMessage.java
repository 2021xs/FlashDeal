package com.flashdeal.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ShopCacheInvalidationMessage {

    private Long shopId;

    private String cacheType;

    private LocalDateTime eventTime;
}
