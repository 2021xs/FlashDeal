package com.flashdeal.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    String key() default "";

    long windowSeconds() default 1;

    long maxRequests() default 100;

    RateLimitDimension dimension() default RateLimitDimension.IP;
}
