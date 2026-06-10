package com.flashdeal.mq;

import com.flashdeal.dto.Result;
import com.flashdeal.dto.UserDTO;
import com.flashdeal.service.IVoucherOrderService;
import com.flashdeal.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

@ActiveProfiles("test")
@SpringBootTest(properties = {
        "seckill.mq-message.retry.enabled=true",
        "seckill.mq-message.retry-interval-seconds=1",
        "seckill.mq-message.sent-timeout-seconds=1"
})
class ConvertAndSendExceptionValidationTest {

    private static final long VOUCHER_ID = 9320L;
    private static final long USER_ID = 99320L;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @MockBean
    private RabbitTemplate rabbitTemplate;

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void convertAndSendThrowsAfterMessageSavedShouldKeepPendingForRetry() throws Exception {
        prepareData();
        doThrow(new AmqpException("test-only convertAndSend failure"))
                .when(rabbitTemplate)
                .convertAndSend(
                        eq("flashdeal.seckill.order.exchange"),
                        eq("flashdeal.seckill.order"),
                        any(Object.class),
                        any(MessagePostProcessor.class),
                        any(CorrelationData.class)
                );

        UserDTO user = new UserDTO();
        user.setId(USER_ID);
        user.setNickName("mq-c-user");
        user.setIcon("");
        UserHolder.saveUser(user);

        Result result = voucherOrderService.seckillVoucher(VOUCHER_ID);

        Assertions.assertTrue(result.getSuccess(), "saved reliable message should return accepted orderId");
        Assertions.assertEquals("0", stringRedisTemplate.opsForValue().get("seckill:stock:" + VOUCHER_ID));
        Assertions.assertTrue(Boolean.TRUE.equals(stringRedisTemplate.opsForSet()
                .isMember("seckill:order:" + VOUCHER_ID, String.valueOf(USER_ID))));

        Integer orderCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE user_id=? AND voucher_id=?",
                Integer.class,
                USER_ID,
                VOUCHER_ID
        );
        Assertions.assertEquals(0, orderCount);

        List<Long> messageIds = jdbcTemplate.queryForList(
                "SELECT id FROM tb_mq_message WHERE message_body LIKE ? ORDER BY create_time DESC LIMIT 1",
                Long.class,
                "%\"voucherId\":" + VOUCHER_ID + "%"
        );
        Assertions.assertFalse(messageIds.isEmpty(), "mq message should be saved before send");
        Long messageId = messageIds.get(0);

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM tb_mq_message WHERE id=?",
                String.class,
                messageId
        );
        Assertions.assertEquals("INIT", status);
        Assertions.assertFalse(stringRedisTemplate.opsForZSet()
                .range("seckill:pending", 0, -1).isEmpty());

        Thread.sleep(2500L);
        String statusAfterRetryWindow = jdbcTemplate.queryForObject(
                "SELECT status FROM tb_mq_message WHERE id=?",
                String.class,
                messageId
        );
        Assertions.assertNotEquals("FAILED", statusAfterRetryWindow, "saved MQ message should remain retryable");

        System.out.println("C_VALIDATION_RESULT messageId=" + messageId
                + ", userId=" + USER_ID
                + ", voucherId=" + VOUCHER_ID
                + ", status=" + statusAfterRetryWindow
                + ", redisStock=" + stringRedisTemplate.opsForValue().get("seckill:stock:" + VOUCHER_ID)
                + ", orderCount=" + orderCount);
    }

    private void prepareData() {
        ensureMqMessageTable();
        jdbcTemplate.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", VOUCHER_ID);
        jdbcTemplate.update("DELETE FROM tb_mq_message WHERE message_body LIKE ?",
                "%\"voucherId\":" + VOUCHER_ID + "%");
        jdbcTemplate.update("DELETE FROM tb_seckill_voucher WHERE voucher_id=?", VOUCHER_ID);
        jdbcTemplate.update("DELETE FROM tb_voucher WHERE id=?", VOUCHER_ID);
        jdbcTemplate.update("INSERT INTO tb_voucher(id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status, create_time, update_time) "
                        + "VALUES (?, 1, 'mq-c', 'mq-c', 'mq-c', 100, 100, 1, 1, NOW(), NOW())",
                VOUCHER_ID);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        jdbcTemplate.update("INSERT INTO tb_seckill_voucher(voucher_id, stock, begin_time, end_time) VALUES (?, 1, ?, ?)",
                VOUCHER_ID,
                LocalDateTime.now().minusHours(1).format(formatter),
                LocalDateTime.now().plusHours(1).format(formatter));
        stringRedisTemplate.delete("seckill:stock:" + VOUCHER_ID);
        stringRedisTemplate.delete("seckill:order:" + VOUCHER_ID);
        stringRedisTemplate.opsForValue().set("seckill:stock:" + VOUCHER_ID, "1");
    }

    private void ensureMqMessageTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS tb_mq_message ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "biz_type VARCHAR(64) NOT NULL,"
                + "biz_id BIGINT NOT NULL,"
                + "exchange_name VARCHAR(128) NOT NULL,"
                + "routing_key VARCHAR(128) NOT NULL,"
                + "message_body TEXT NOT NULL,"
                + "status VARCHAR(32) NOT NULL,"
                + "retry_count INT NOT NULL DEFAULT 0,"
                + "max_retry_count INT NOT NULL DEFAULT 3,"
                + "next_retry_time DATETIME DEFAULT NULL,"
                + "fail_reason VARCHAR(512) DEFAULT NULL,"
                + "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "KEY idx_biz (biz_type, biz_id),"
                + "KEY idx_status_retry (status, next_retry_time, retry_count),"
                + "KEY idx_update_time (update_time)"
                + ")");
    }
}
