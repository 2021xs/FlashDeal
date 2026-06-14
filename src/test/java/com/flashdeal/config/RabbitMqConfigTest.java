package com.flashdeal.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLK;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLX;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_ROUTING_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class RabbitMqConfigTest {

    @Test
    void orderCloseQueueShouldHaveDeadLetterConfig() {
        RabbitMqConfig config = new RabbitMqConfig();
        ReflectionTestUtils.setField(config, "orderTimeoutSeconds", 900L);

        Queue queue = config.orderCloseQueue();

        Map<String, Object> arguments = queue.getArguments();
        assertEquals(ORDER_CLOSE_DLX, arguments.get("x-dead-letter-exchange"));
        assertEquals(ORDER_CLOSE_DLK, arguments.get("x-dead-letter-routing-key"));
    }

    @Test
    void seckillOrderBatchFactoryShouldReadBatchConsumeConfig() {
        RabbitMqConfig config = new RabbitMqConfig();
        ReflectionTestUtils.setField(config, "seckillOrderBatchSize", 100);
        ReflectionTestUtils.setField(config, "seckillOrderBatchReceiveTimeoutMs", 100L);
        ReflectionTestUtils.setField(config, "seckillOrderBatchPrefetch", 100);
        ReflectionTestUtils.setField(config, "seckillOrderBatchConcurrency", 1);
        ReflectionTestUtils.setField(config, "seckillOrderBatchMaxConcurrency", 1);

        SimpleRabbitListenerContainerFactory factory =
                config.seckillOrderBatchRabbitListenerContainerFactory(mock(ConnectionFactory.class));

        assertEquals(100, ReflectionTestUtils.getField(factory, "batchSize"));
        assertEquals(100L, ReflectionTestUtils.getField(factory, "receiveTimeout"));
        assertEquals(100, ReflectionTestUtils.getField(factory, "prefetchCount"));
        assertEquals(1, ReflectionTestUtils.getField(factory, "concurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(factory, "maxConcurrentConsumers"));
        assertTrue((Boolean) ReflectionTestUtils.getField(factory, "batchListener"));
        assertTrue((Boolean) ReflectionTestUtils.getField(factory, "consumerBatchEnabled"));
    }

    @Test
    void seckillClaimRetryQueueShouldDelayThenDeadLetterBackToOrderQueue() {
        RabbitMqConfig config = new RabbitMqConfig();
        ReflectionTestUtils.setField(config, "seckillClaimRetryDelayMillis", 3000L);

        Queue queue = config.seckillClaimRetryQueue();

        Map<String, Object> arguments = queue.getArguments();
        assertEquals(3000L, arguments.get("x-message-ttl"));
        assertEquals(SECKILL_ORDER_EXCHANGE, arguments.get("x-dead-letter-exchange"));
        assertEquals(SECKILL_ORDER_ROUTING_KEY, arguments.get("x-dead-letter-routing-key"));
    }
}
