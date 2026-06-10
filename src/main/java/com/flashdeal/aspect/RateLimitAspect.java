package com.flashdeal.aspect;

import cn.hutool.core.util.StrUtil;
import com.flashdeal.annotation.RateLimit;
import com.flashdeal.annotation.RateLimitDimension;
import com.flashdeal.dto.Result;
import com.flashdeal.dto.UserDTO;
import com.flashdeal.utils.RedisConstants;
import com.flashdeal.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.UUID;

@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    private static final String USER_PREFIX = "user:";
    private static final String IP_PREFIX = "ip:";
    private static final String API_PREFIX = "api:";
    private static final String UNKNOWN_IP = "unknown";
    private static final String RATE_LIMIT_MESSAGE = "请求过于频繁，请稍后再试";
    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("rate_limit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return joinPoint.proceed();
        }

        HttpServletRequest request = attributes.getRequest();
        String redisKey = buildRedisKey(rateLimit, request);
        long nowMillis = System.currentTimeMillis();
        long windowMillis = rateLimit.windowSeconds() * 1000;
        long expireSeconds = rateLimit.windowSeconds() + 1;
        String requestId = nowMillis + ":" + UUID.randomUUID();

        Long allowed = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Collections.singletonList(redisKey),
                String.valueOf(nowMillis),
                String.valueOf(windowMillis),
                String.valueOf(rateLimit.maxRequests()),
                requestId,
                String.valueOf(expireSeconds)
        );

        if (allowed == null || allowed == 0L) {
            log.warn("Rate limit rejected, key={}, dimension={}, windowSeconds={}, maxRequests={}",
                    redisKey, rateLimit.dimension(), rateLimit.windowSeconds(), rateLimit.maxRequests());
            return Result.fail(RATE_LIMIT_MESSAGE);
        }

        log.debug("Rate limit passed, key={}, dimension={}", redisKey, rateLimit.dimension());
        return joinPoint.proceed();
    }

    private String buildRedisKey(RateLimit rateLimit, HttpServletRequest request) {
        String businessKey = StrUtil.isBlank(rateLimit.key()) ? request.getRequestURI() : rateLimit.key();
        String dimensionValue = buildDimensionValue(rateLimit.dimension(), request);
        return RedisConstants.RATE_LIMIT_KEY
                + rateLimit.dimension().name().toLowerCase()
                + ":"
                + businessKey
                + ":"
                + dimensionValue;
    }

    private String buildDimensionValue(RateLimitDimension dimension, HttpServletRequest request) {
        if (dimension == RateLimitDimension.USER) {
            UserDTO user = UserHolder.getUser();
            if (user != null && user.getId() != null) {
                return USER_PREFIX + user.getId();
            }
            return IP_PREFIX + getClientIp(request);
        }
        if (dimension == RateLimitDimension.API) {
            return API_PREFIX + request.getRequestURI();
        }
        return IP_PREFIX + getClientIp(request);
    }

    private String getClientIp(HttpServletRequest request) {
        String remoteAddr = request.getRemoteAddr();
        return StrUtil.isBlank(remoteAddr) ? UNKNOWN_IP : remoteAddr;
    }
}
