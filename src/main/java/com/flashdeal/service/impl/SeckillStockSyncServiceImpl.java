package com.flashdeal.service.impl;

import com.flashdeal.dto.SeckillStockSyncResult;
import com.flashdeal.entity.SeckillVoucher;
import com.flashdeal.service.ISeckillVoucherService;
import com.flashdeal.service.SeckillStockSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

import static com.flashdeal.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Service
public class SeckillStockSyncServiceImpl implements SeckillStockSyncService, ApplicationRunner {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Value("${seckill.stock-sync.enabled:true}")
    private boolean stockSyncEnabled;

    @Value("${seckill.stock-sync.force:false}")
    private boolean stockSyncForce;

    @Override
    public void run(ApplicationArguments args) {
        if (!stockSyncEnabled) {
            log.info("Seckill stock startup sync disabled");
            return;
        }
        SeckillStockSyncResult result = syncActiveSeckillStock(stockSyncForce);
        log.info("Seckill stock startup sync finished, force={}, synced={}, skipped={}, failed={}",
                stockSyncForce, result.getSyncedCount(), result.getSkippedCount(), result.getFailedCount());
    }

    @Override
    public SeckillStockSyncResult syncActiveSeckillStock(boolean force) {
        SeckillStockSyncResult result = new SeckillStockSyncResult();
        LocalDateTime now = LocalDateTime.now();
        List<SeckillVoucher> vouchers = seckillVoucherService.query()
                .ge("stock", 0)
                .gt("end_time", now)
                .list();
        for (SeckillVoucher voucher : vouchers) {
            syncOne(voucher, force, result);
        }
        return result;
    }

    private void syncOne(SeckillVoucher voucher, boolean force, SeckillStockSyncResult result) {
        Long voucherId = voucher.getVoucherId();
        String key = SECKILL_STOCK_KEY + voucherId;
        try {
            Boolean exists = stringRedisTemplate.hasKey(key);
            if (Boolean.TRUE.equals(exists) && !force) {
                result.addSkipped();
                log.debug("Skip existing seckill stock key, voucherId={}, key={}", voucherId, key);
                return;
            }
            stringRedisTemplate.opsForValue().set(key, String.valueOf(voucher.getStock()));
            result.addSynced();
            log.info("Synced seckill stock to Redis, voucherId={}, stock={}, force={}",
                    voucherId, voucher.getStock(), force);
        } catch (Exception e) {
            result.addFailed();
            log.error("Sync seckill stock failed, voucherId={}, key={}", voucherId, key, e);
        }
    }
}
