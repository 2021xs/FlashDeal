package com.flashdeal.dto;

import lombok.Data;

@Data
public class SeckillOrderResultDTO {

    private Long orderId;

    private String result;

    private Integer orderStatus;

    private String mqStatus;

    private String message;
}
