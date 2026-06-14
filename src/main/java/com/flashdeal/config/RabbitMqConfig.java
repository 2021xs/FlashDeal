package com.flashdeal.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.retry.ImmediateRequeueMessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

import java.util.HashMap;
import java.util.Map;

import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLK;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLQ;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_DLX;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_QUEUE;
import static com.flashdeal.utils.RabbitConstants.ORDER_CLOSE_ROUTING_KEY;
import static com.flashdeal.utils.RabbitConstants.ORDER_TIMEOUT_DELAY_QUEUE;
import static com.flashdeal.utils.RabbitConstants.ORDER_TIMEOUT_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.ORDER_TIMEOUT_ROUTING_KEY;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLQ;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLK;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_DLX;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_EXCHANGE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_QUEUE;
import static com.flashdeal.utils.RabbitConstants.SECKILL_ORDER_ROUTING_KEY;
import static com.flashdeal.utils.RabbitConstants.SHOP_CACHE_INVALIDATE_EXCHANGE;

@Configuration
public class RabbitMqConfig {

    @Value("${order.timeout.seconds:900}")
    private long orderTimeoutSeconds;

    @Value("${spring.rabbitmq.listener.simple.concurrency:1}")
    private int rabbitListenerConcurrency;

    @Value("${spring.rabbitmq.listener.simple.max-concurrency:1}")
    private int rabbitListenerMaxConcurrency;

    @Value("${spring.rabbitmq.listener.simple.prefetch:1}")
    private int rabbitListenerPrefetch;

    @Value("${seckill.order.batch-consume.batch-size:100}")
    private int seckillOrderBatchSize;

    @Value("${seckill.order.batch-consume.receive-timeout-ms:100}")
    private long seckillOrderBatchReceiveTimeoutMs;

    @Value("${seckill.order.batch-consume.prefetch:100}")
    private int seckillOrderBatchPrefetch;

    @Value("${seckill.order.batch-consume.concurrency:1}")
    private int seckillOrderBatchConcurrency;

    @Value("${seckill.order.batch-consume.max-concurrency:1}")
    private int seckillOrderBatchMaxConcurrency;

    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(SECKILL_ORDER_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillOrderQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", SECKILL_ORDER_DLX);
        args.put("x-dead-letter-routing-key", SECKILL_ORDER_DLK);
        return new Queue(SECKILL_ORDER_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding seckillOrderBinding(Queue seckillOrderQueue, DirectExchange seckillOrderExchange) {
        return BindingBuilder.bind(seckillOrderQueue)
                .to(seckillOrderExchange)
                .with(SECKILL_ORDER_ROUTING_KEY);
    }

    @Bean
    public DirectExchange seckillOrderDeadLetterExchange() {
        return new DirectExchange(SECKILL_ORDER_DLX, true, false);
    }

    @Bean
    public Queue seckillOrderDeadLetterQueue() {
        return new Queue(SECKILL_ORDER_DLQ, true);
    }

    @Bean
    public Binding seckillOrderDeadLetterBinding(
            Queue seckillOrderDeadLetterQueue,
            DirectExchange seckillOrderDeadLetterExchange) {
        return BindingBuilder.bind(seckillOrderDeadLetterQueue)
                .to(seckillOrderDeadLetterExchange)
                .with(SECKILL_ORDER_DLK);
    }

    @Bean
    public FanoutExchange shopCacheInvalidationExchange() {
        return new FanoutExchange(SHOP_CACHE_INVALIDATE_EXCHANGE, true, false);
    }

    @Bean
    public Queue shopCacheInvalidationQueue() {
        return new AnonymousQueue();
    }

    @Bean
    public Binding shopCacheInvalidationBinding(
            Queue shopCacheInvalidationQueue,
            FanoutExchange shopCacheInvalidationExchange) {
        return BindingBuilder.bind(shopCacheInvalidationQueue)
                .to(shopCacheInvalidationExchange);
    }

    @Bean
    public DirectExchange orderTimeoutExchange() {
        return new DirectExchange(ORDER_TIMEOUT_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderTimeoutDelayQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", orderTimeoutSeconds * 1000);
        args.put("x-dead-letter-exchange", ORDER_CLOSE_EXCHANGE);
        args.put("x-dead-letter-routing-key", ORDER_CLOSE_ROUTING_KEY);
        return new Queue(ORDER_TIMEOUT_DELAY_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding orderTimeoutBinding(
            Queue orderTimeoutDelayQueue,
            DirectExchange orderTimeoutExchange) {
        return BindingBuilder.bind(orderTimeoutDelayQueue)
                .to(orderTimeoutExchange)
                .with(ORDER_TIMEOUT_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderCloseExchange() {
        return new DirectExchange(ORDER_CLOSE_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderCloseQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", ORDER_CLOSE_DLX);
        args.put("x-dead-letter-routing-key", ORDER_CLOSE_DLK);
        return new Queue(ORDER_CLOSE_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding orderCloseBinding(
            Queue orderCloseQueue,
            DirectExchange orderCloseExchange) {
        return BindingBuilder.bind(orderCloseQueue)
                .to(orderCloseExchange)
                .with(ORDER_CLOSE_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderCloseDeadLetterExchange() {
        return new DirectExchange(ORDER_CLOSE_DLX, true, false);
    }

    @Bean
    public Queue orderCloseDeadLetterQueue() {
        return new Queue(ORDER_CLOSE_DLQ, true);
    }

    @Bean
    public Binding orderCloseDeadLetterBinding(
            Queue orderCloseDeadLetterQueue,
            DirectExchange orderCloseDeadLetterExchange) {
        return BindingBuilder.bind(orderCloseDeadLetterQueue)
                .to(orderCloseDeadLetterExchange)
                .with(ORDER_CLOSE_DLK);
    }

    @Bean
    public MessageConverter messageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter);
        rabbitTemplate.setMandatory(true);
        return rabbitTemplate;
    }

    @Bean
    public RetryOperationsInterceptor rabbitRetryInterceptor() {
        return RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(1000, 2.0, 5000)
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build();
    }

    @Bean
    public RetryOperationsInterceptor dlqRabbitRetryInterceptor() {
        return RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(1000, 2.0, 5000)
                .recoverer(new ImmediateRequeueMessageRecoverer())
                .build();
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter,
            @Qualifier("rabbitRetryInterceptor") RetryOperationsInterceptor rabbitRetryInterceptor) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        factory.setAdviceChain(rabbitRetryInterceptor);
        factory.setConcurrentConsumers(rabbitListenerConcurrency);
        factory.setMaxConcurrentConsumers(rabbitListenerMaxConcurrency);
        factory.setPrefetchCount(rabbitListenerPrefetch);
        return factory;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory dlqRabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter,
            @Qualifier("dlqRabbitRetryInterceptor") RetryOperationsInterceptor dlqRabbitRetryInterceptor) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(true);
        factory.setAdviceChain(dlqRabbitRetryInterceptor);
        factory.setConcurrentConsumers(rabbitListenerConcurrency);
        factory.setMaxConcurrentConsumers(rabbitListenerMaxConcurrency);
        factory.setPrefetchCount(rabbitListenerPrefetch);
        return factory;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory seckillOrderBatchRabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        factory.setBatchListener(true);
        factory.setConsumerBatchEnabled(true);
        factory.setBatchSize(seckillOrderBatchSize);
        factory.setReceiveTimeout(seckillOrderBatchReceiveTimeoutMs);
        factory.setPrefetchCount(Math.max(seckillOrderBatchPrefetch, seckillOrderBatchSize));
        factory.setConcurrentConsumers(seckillOrderBatchConcurrency);
        factory.setMaxConcurrentConsumers(seckillOrderBatchMaxConcurrency);
        return factory;
    }
}
