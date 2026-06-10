package com.flashdeal.controller;


import com.flashdeal.annotation.RateLimit;
import com.flashdeal.annotation.RateLimitDimension;
import com.flashdeal.dto.Result;
import com.flashdeal.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    @RateLimit(key = "seckill:voucher", windowSeconds = 5, maxRequests = 3, dimension = RateLimitDimension.USER)
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    @GetMapping("seckill/result/{orderId}")
    public Result querySeckillResult(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.querySeckillResult(orderId);
    }

    @PostMapping("pay/{orderId}")
    public Result payOrder(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.payOrder(orderId);
    }

    @PostMapping("use/{orderId}")
    public Result useOrder(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.useOrder(orderId);
    }
}
