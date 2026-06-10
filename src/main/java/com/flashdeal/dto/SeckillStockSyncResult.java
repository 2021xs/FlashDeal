package com.flashdeal.dto;

import lombok.Data;

@Data
public class SeckillStockSyncResult {

    private int syncedCount;

    private int skippedCount;

    private int failedCount;

    public void addSynced() {
        syncedCount++;
    }

    public void addSkipped() {
        skippedCount++;
    }

    public void addFailed() {
        failedCount++;
    }
}
