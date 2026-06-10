package com.flashdeal.service;

import com.flashdeal.dto.SeckillStockSyncResult;

public interface SeckillStockSyncService {

    SeckillStockSyncResult syncActiveSeckillStock(boolean force);
}
