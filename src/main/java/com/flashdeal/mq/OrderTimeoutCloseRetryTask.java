package com.flashdeal.mq;

import com.flashdeal.entity.OrderTimeoutCloseFail;
import com.flashdeal.service.IOrderTimeoutCloseFailService;
import com.flashdeal.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class OrderTimeoutCloseRetryTask {

    @Resource
    private IOrderTimeoutCloseFailService failService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Value("${order-timeout.close-retry.enabled:true}")
    private boolean enabled;

    @Value("${order-timeout.close-retry.batch-size:100}")
    private int batchSize;

    @Value("${order-timeout.close-retry.base-delay-seconds:60}")
    private long baseRetryDelaySeconds;

    @Value("${order-timeout.close-retry.max-delay-seconds:3600}")
    private long maxRetryDelaySeconds;

    @Value("${order-timeout.close-retry.processing-timeout-seconds:300}")
    private long processingTimeoutSeconds;

    @Scheduled(fixedDelayString = "#{${order-timeout.close-retry.fixed-delay-seconds:60} * 1000}")
    public void retryFailedOrderClose() {
        if (!enabled) {
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        failService.recoverStuckProcessing(
                now.minusSeconds(Math.max(1L, processingTimeoutSeconds)), now, Math.max(1, batchSize));
        List<OrderTimeoutCloseFail> failures = failService.listRetryable(now, Math.max(1, batchSize));
        for (OrderTimeoutCloseFail failure : failures) {
            retryOne(failure);
        }
    }

    void retryOne(OrderTimeoutCloseFail failure) {
        if (!failService.claimRetry(failure)) {
            return;
        }
        try {
            voucherOrderService.closeTimeoutOrder(failure.getOrderId());
            boolean handled = failService.markHandled(failure);
            log.info("Order timeout close retry handled, orderId={}, failCount={}, updated={}",
                    failure.getOrderId(), failure.getFailCount(), handled);
        } catch (Exception e) {
            boolean updated = failService.markRetryFailed(
                    failure, summarizeException(e), baseRetryDelaySeconds, maxRetryDelaySeconds);
            log.error("Order timeout close retry failed, orderId={}, failCount={}, updated={}",
                    failure.getOrderId(), failure.getFailCount(), updated, e);
        }
    }

    private String summarizeException(Throwable e) {
        String message = e.getMessage();
        String summary = e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return summary.length() <= 512 ? summary : summary.substring(0, 512);
    }
}
