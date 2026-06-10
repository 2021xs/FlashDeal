package com.flashdeal.config;

import com.flashdeal.dto.Result;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class WebExceptionAdviceTest {

    @Test
    void shouldReturnDuplicateOrderMessageForDuplicateKeyException() {
        WebExceptionAdvice advice = new WebExceptionAdvice();

        Result result = advice.handleDuplicateKeyException(new DuplicateKeyException("uk_user_voucher"));

        assertFalse(result.getSuccess());
        assertEquals("不能重复下单", result.getErrorMsg());
    }
}
