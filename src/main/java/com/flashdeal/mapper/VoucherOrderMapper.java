package com.flashdeal.mapper;

import com.flashdeal.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    int insertBatch(@Param("orders") List<VoucherOrder> orders);

    List<VoucherOrder> selectExistingUserVoucherPairs(@Param("orders") List<VoucherOrder> orders);
}
