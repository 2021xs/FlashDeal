package com.flashdeal.enums;

public enum VoucherOrderStatus {

    UNPAID(1, "未支付"),
    PAID(2, "已支付"),
    USED(3, "已核销"),
    CANCELED(4, "已取消"),
    REFUNDING(5, "退款中"),
    REFUNDED(6, "已退款");

    private final int code;
    private final String desc;

    VoucherOrderStatus(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static VoucherOrderStatus fromCode(Integer code) {
        if (code == null) {
            return null;
        }
        for (VoucherOrderStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        return null;
    }
}
