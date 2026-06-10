package com.flashdeal.mq;

import com.flashdeal.dto.ShopCacheInvalidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;

import static com.flashdeal.utils.RabbitConstants.SHOP_CACHE_INVALIDATE_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SHOP_CACHE_TYPE;

@Slf4j
@Component
public class ShopCacheInvalidationProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    public void sendShopCacheInvalidation(Long shopId) {
        ShopCacheInvalidationMessage message = new ShopCacheInvalidationMessage();
        message.setShopId(shopId);
        message.setCacheType(SHOP_CACHE_TYPE);
        message.setEventTime(LocalDateTime.now());
        try {
            rabbitTemplate.convertAndSend(SHOP_CACHE_INVALIDATE_EXCHANGE, "", message);
            log.info("Shop cache invalidation message sent, shopId={}", shopId);
        } catch (Exception e) {
            log.error("Shop cache invalidation message send failed, shopId={}", shopId, e);
        }
    }
}
