package com.flashdeal.service;

import com.flashdeal.dto.Result;
import com.flashdeal.dto.BatchVoucherOrderResult;
import com.flashdeal.entity.VoucherOrder;
import com.flashdeal.dto.VoucherOrderMessage;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result querySeckillResult(Long orderId);

    BatchVoucherOrderResult createVoucherOrdersBatch(List<VoucherOrder> voucherOrders);

    BatchVoucherOrderResult createClaimedVoucherOrdersBatch(List<VoucherOrderMessage> messages);

    boolean hasExistingOrder(Long orderId, Long userId, Long voucherId);

    Result payOrder(Long orderId);

    Result useOrder(Long orderId);

    void closeTimeoutOrder(Long orderId);

    boolean closeUnpaidOrderIfNecessary(Long orderId);
}
