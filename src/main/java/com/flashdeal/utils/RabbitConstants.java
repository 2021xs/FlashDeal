package com.flashdeal.utils;

public class RabbitConstants {

    public static final String SECKILL_ORDER_EXCHANGE = "flashdeal.seckill.order.exchange";
    public static final String SECKILL_ORDER_QUEUE = "flashdeal.seckill.order.queue";
    public static final String SECKILL_ORDER_ROUTING_KEY = "flashdeal.seckill.order";

    public static final String SECKILL_ORDER_DLX = "flashdeal.seckill.order.dlx";
    public static final String SECKILL_ORDER_DLQ = "flashdeal.seckill.order.dlq";
    public static final String SECKILL_ORDER_DLK = "flashdeal.seckill.order.dead";
    public static final String SECKILL_CLAIM_RETRY_EXCHANGE = "flashdeal.seckill.claim.retry.exchange";
    public static final String SECKILL_CLAIM_RETRY_QUEUE = "flashdeal.seckill.claim.retry.queue";
    public static final String SECKILL_CLAIM_RETRY_ROUTING_KEY = "flashdeal.seckill.claim.retry";

    public static final String SHOP_CACHE_INVALIDATE_EXCHANGE = "flashdeal.shop.cache.invalidate.exchange";
    public static final String SHOP_CACHE_TYPE = "SHOP";

    public static final String ORDER_TIMEOUT_EXCHANGE = "flashdeal.order.timeout.exchange";
    public static final String ORDER_TIMEOUT_DELAY_QUEUE = "flashdeal.order.timeout.delay.queue";
    public static final String ORDER_TIMEOUT_ROUTING_KEY = "flashdeal.order.timeout";

    public static final String ORDER_CLOSE_EXCHANGE = "flashdeal.order.close.exchange";
    public static final String ORDER_CLOSE_QUEUE = "flashdeal.order.close.queue";
    public static final String ORDER_CLOSE_ROUTING_KEY = "flashdeal.order.close";
    public static final String ORDER_CLOSE_DLX = "flashdeal.order.close.dlx";
    public static final String ORDER_CLOSE_DLQ = "flashdeal.order.close.dlq";
    public static final String ORDER_CLOSE_DLK = "flashdeal.order.close.dead";

    private RabbitConstants() {
    }
}
