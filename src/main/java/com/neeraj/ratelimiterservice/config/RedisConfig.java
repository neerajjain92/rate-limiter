package com.neeraj.ratelimiterservice.config;

import com.neeraj.ratelimiterservice.RateLimiterService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    @Value("${redis.uri}")
    private String redisUri;

    @Bean
    public RateLimiterService rateLimiterService() {
        return new RateLimiterService(redisUri);
    }
}
