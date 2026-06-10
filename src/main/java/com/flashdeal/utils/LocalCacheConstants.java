package com.flashdeal.utils;

public class LocalCacheConstants {

    private LocalCacheConstants() {
    }

    public static final String SHOP_LOCAL_CACHE_NAME = "shopLocalCache";
    public static final long SHOP_LOCAL_CACHE_MAX_SIZE = 10000L;
    public static final long SHOP_LOCAL_CACHE_TTL_MINUTES = 5L;
    public static final long SHOP_REDIS_RANDOM_TTL_MINUTES = 5L;
}
