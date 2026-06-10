package com.flashdeal.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.flashdeal.entity.Shop;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

import static com.flashdeal.utils.LocalCacheConstants.SHOP_LOCAL_CACHE_MAX_SIZE;
import static com.flashdeal.utils.LocalCacheConstants.SHOP_LOCAL_CACHE_TTL_MINUTES;

@Configuration
public class CaffeineConfig {

    @Bean
    public Cache<Long, Shop> shopLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(SHOP_LOCAL_CACHE_MAX_SIZE)
                .expireAfterWrite(SHOP_LOCAL_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
                .build();
    }
}
