package com.flashdeal.mq;

import com.github.benmanes.caffeine.cache.Cache;
import com.flashdeal.dto.ShopCacheInvalidationMessage;
import com.flashdeal.entity.Shop;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.flashdeal.utils.RabbitConstants.SHOP_CACHE_TYPE;

@Slf4j
@Component
public class ShopCacheInvalidationConsumer {

    @Resource
    private Cache<Long, Shop> shopLocalCache;

    @RabbitListener(queues = "#{shopCacheInvalidationQueue.name}", containerFactory = "rabbitListenerContainerFactory")
    public void handleShopCacheInvalidation(
            ShopCacheInvalidationMessage invalidationMessage,
            Message message,
            Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            log.info("Shop cache invalidation message received, shopId={}, cacheType={}, eventTime={}",
                    invalidationMessage.getShopId(),
                    invalidationMessage.getCacheType(),
                    invalidationMessage.getEventTime());
            if (!SHOP_CACHE_TYPE.equals(invalidationMessage.getCacheType())) {
                log.warn("Unknown cache invalidation type ignored, cacheType={}, shopId={}",
                        invalidationMessage.getCacheType(),
                        invalidationMessage.getShopId());
                channel.basicAck(deliveryTag, false);
                return;
            }
            shopLocalCache.invalidate(invalidationMessage.getShopId());
            channel.basicAck(deliveryTag, false);
            log.info("Shop local cache invalidated by MQ, shopId={}", invalidationMessage.getShopId());
        } catch (Exception e) {
            log.error("Shop cache invalidation consume failed, shopId={}",
                    invalidationMessage == null ? null : invalidationMessage.getShopId(), e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
