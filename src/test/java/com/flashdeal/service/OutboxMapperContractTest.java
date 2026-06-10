package com.flashdeal.service;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxMapperContractTest {

    @Test
    void mapperShouldProtectClaimAndMoveExhaustedRetryToNeedManual() throws Exception {
        String xml = new String(
                Files.readAllBytes(new ClassPathResource("mapper/OutboxEventMapper.xml").getFile().toPath()),
                StandardCharsets.UTF_8);

        assertTrue(xml.contains("status = #{expectedStatus}"));
        assertTrue(xml.contains("retry_count = #{expectedRetryCount}"));
        assertTrue(xml.contains("retry_count + 1 &gt;= max_retry_count THEN 'NEED_MANUAL'"));
        assertTrue(xml.contains("WHERE id = #{id}"));
        assertTrue(xml.contains("AND status = 'SENDING'"));
    }
}
