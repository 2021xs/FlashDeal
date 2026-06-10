package com.flashdeal.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.flashdeal.dto.Result;
import com.flashdeal.entity.Shop;
import com.flashdeal.mapper.ShopMapper;
import com.flashdeal.mq.ShopCacheInvalidationProducer;
import com.flashdeal.service.IShopService;
import com.flashdeal.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.flashdeal.utils.LocalCacheConstants.SHOP_REDIS_RANDOM_TTL_MINUTES;
import static com.flashdeal.utils.RedisConstants.CACHE_NULL_TTL;
import static com.flashdeal.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.flashdeal.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.flashdeal.utils.RedisConstants.SHOP_GEO_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private Cache<Long, Shop> shopLocalCache;

    @Resource
    private ShopCacheInvalidationProducer shopCacheInvalidationProducer;

    @Value("${local-cache.shop.enabled:true}")
    private boolean shopLocalCacheEnabled;

    private final AtomicLong caffeineHit = new AtomicLong();
    private final AtomicLong redisHit = new AtomicLong();
    private final AtomicLong mysqlHit = new AtomicLong();

    @Override
    public Result queryById(Long id) {
        Shop shop = queryShopWithTwoLevelCache(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        boolean success = updateById(shop);
        if (!success) {
            return Result.fail("店铺更新失败");
        }
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        shopLocalCache.invalidate(id);
        log.info("Shop cache invalidated, shopId={}", id);
        shopCacheInvalidationProducer.sendShopCacheInvalidation(id);
        return Result.ok();
    }

    private Shop queryShopWithTwoLevelCache(Long id) {
        if (shopLocalCacheEnabled) {
            Shop localShop = shopLocalCache.getIfPresent(id);
            if (localShop != null) {
                long hit = caffeineHit.incrementAndGet();
                log.info("Shop cache hit Caffeine, shopId={}, caffeineHit={}", id, hit);
                return localShop;
            }
        } else {
            log.info("Shop local cache disabled, shopId={}", id);
        }

        String key = CACHE_SHOP_KEY + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            Shop shop = JSONUtil.toBean(json, Shop.class);
            if (shopLocalCacheEnabled) {
                shopLocalCache.put(id, shop);
            }
            long hit = redisHit.incrementAndGet();
            log.info("Shop cache hit Redis, shopId={}, redisHit={}", id, hit);
            return shop;
        }
        if (json != null) {
            long hit = redisHit.incrementAndGet();
            log.info("Shop cache hit Redis null, shopId={}, redisHit={}", id, hit);
            return null;
        }

        Shop shop = getById(id);
        long hit = mysqlHit.incrementAndGet();
        log.info("Shop cache miss, query MySQL, shopId={}, mysqlHit={}", id, hit);
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        long ttl = CACHE_SHOP_TTL + ThreadLocalRandom.current().nextLong(SHOP_REDIS_RANDOM_TTL_MINUTES + 1);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), ttl, TimeUnit.MINUTES);
        if (shopLocalCacheEnabled) {
            shopLocalCache.put(id, shop);
        }
        return shop;
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
