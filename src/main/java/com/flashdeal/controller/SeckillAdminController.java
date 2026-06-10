package com.flashdeal.controller;

import com.flashdeal.dto.Result;
import com.flashdeal.service.SeckillStockSyncService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/seckill/admin")
public class SeckillAdminController {

    @Resource
    private SeckillStockSyncService seckillStockSyncService;

    @PostMapping("/stock/sync")
    public Result syncStock(@RequestParam(value = "force", defaultValue = "false") boolean force) {
        return Result.ok(seckillStockSyncService.syncActiveSeckillStock(force));
    }
}
