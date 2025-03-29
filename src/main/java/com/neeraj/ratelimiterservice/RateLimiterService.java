package com.neeraj.ratelimiterservice;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class RateLimiterService {

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> syncCommands;
    private final String rateLimiterScript;
    private final String scriptSha;

    // Cache for rate limit configurations (could be loaded from DB)
    private final Map<String, RateLimitConfig> rateLimitConfigs = new ConcurrentHashMap<>();

    // Cache for blocked IPs
    private final Map<String, Long> blockedIps = new ConcurrentHashMap<>();

    // Scheduler for cleaning up expired blocks
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public RateLimiterService(String redisUri) {
        this.redisClient = RedisClient.create(redisUri);
        this.connection = redisClient.connect();
        this.syncCommands = connection.sync();

        // Initialize with default rate limit configurations
        initRateLimitConfigs();

        // Define the Lua script for rate limiting
        this.rateLimiterScript = "" +
                "local key = KEYS[1] " +
                "local max_requests = tonumber(ARGV[1]) " +
                "local window_seconds = tonumber(ARGV[2]) " +
                "local current_time = tonumber(ARGV[3]) " +

                "-- Add the current request with timestamp as score " +
                "redis.call('ZADD', key, current_time, current_time .. '-' .. math.random()) " +

                "-- Remove elements outside the window " +
                "redis.call('ZREMRANGEBYSCORE', key, 0, current_time - window_seconds) " +

                "-- Count the elements in the window " +
                "local count = redis.call('ZCARD', key) " +

                "-- Set expiration on the key " +
                "redis.call('EXPIRE', key, window_seconds) " +

                "-- Return the count " +
                "return count";

        // Load the script into Redis
        this.scriptSha = syncCommands.scriptLoad(rateLimiterScript);

        // Schedule cleanup of expired blocks
        scheduler.scheduleAtFixedRate(this::cleanupExpiredBlocks, 5, 5, TimeUnit.MINUTES);
    }

    /**
     * Initialize rate limit configurations (in real-world, load from DB)
     */
    private void initRateLimitConfigs() {
        // Format: "requests:window_seconds"
        rateLimitConfigs.put("RATE_LIMIT_PER_SECOND", new RateLimitConfig(100, 1));       // 100 req/second
        rateLimitConfigs.put("RATE_LIMIT_PER_MINUTE", new RateLimitConfig(5000, 60));     // 5K req/minute
        rateLimitConfigs.put("RATE_LIMIT_PER_30_MIN", new RateLimitConfig(30000, 1800));  // 30K req/30 minutes
    }

    /**
     * Check if a request should be allowed or blocked
     * @param ipAddress the client IP address
     * @return true if request is allowed, false if it should be blocked
     */
    public boolean isAllowed(String ipAddress) {
        // First check if IP is already blocked
        if (isIpBlocked(ipAddress)) {
            return false;
        }

        // Check against all rate limit configurations
        for (Map.Entry<String, RateLimitConfig> entry : rateLimitConfigs.entrySet()) {
            String configName = entry.getKey();
            RateLimitConfig config = entry.getValue();

            // Generate a unique key for this IP and rate limit config
            String redisKey = "rate_limit:" + configName + ":" + ipAddress;

            // So this generates 3 different key for 3 different bucket
//            rate_limit:RATE_LIMIT_PER_SECOND:ip_address (keeps 1-second window data)
//            rate_limit:RATE_LIMIT_PER_MINUTE:ip_address (keeps 60-second window data)
//            rate_limit:RATE_LIMIT_PER_30_MIN:ip_address (keeps 30-minute window data)

            // Get current count for this window
            int count = checkRateLimit(redisKey, config.maxRequests, config.windowSeconds);

            // If count exceeds limit, block the IP
            if (count > config.maxRequests) {
                blockIp(ipAddress, configName, config.windowSeconds * 2); // Block for 2x the window time
                return false;
            }
        }

        return true;
    }

    /**
     * Check rate limit for a specific key
     * @return current count in the window
     */
    private int checkRateLimit(String key, int maxRequests, int windowSeconds) {
        long currentTime = Instant.now().getEpochSecond();

        // Execute the Lua script
        Long count = syncCommands.evalsha(
                scriptSha,
                ScriptOutputType.INTEGER,
                new String[]{key},
                String.valueOf(maxRequests),
                String.valueOf(windowSeconds),
                String.valueOf(currentTime)
        );

        return count.intValue();
    }

    /**
     * Block an IP address for a specified duration
     */
    private void blockIp(String ipAddress, String violatedRule, int blockDurationSeconds) {
        long expirationTime = Instant.now().getEpochSecond() + blockDurationSeconds;
        blockedIps.put(ipAddress, expirationTime);

        // Also add to Redis for distributed environments
        syncCommands.setex("blocked_ip:" + ipAddress, blockDurationSeconds, violatedRule);

        // Log the blocking event
        System.out.println("Blocked IP " + ipAddress + " for " + blockDurationSeconds +
                " seconds due to violation of " + violatedRule);
    }

    /**
     * Check if an IP is currently blocked
     */
    private boolean isIpBlocked(String ipAddress) {
        // Check local cache first
        if (blockedIps.containsKey(ipAddress)) {
            long expirationTime = blockedIps.get(ipAddress);
            if (Instant.now().getEpochSecond() < expirationTime) {
                return true;
            } else {
                // Expired block
                blockedIps.remove(ipAddress);
                return false;
            }
        }

        // Check Redis (for distributed environments)
        String result = syncCommands.get("blocked_ip:" + ipAddress);
        return result != null;
    }

    /**
     * Clean up expired IP blocks from local cache
     */
    private void cleanupExpiredBlocks() {
        long currentTime = Instant.now().getEpochSecond();
        blockedIps.entrySet().removeIf(entry -> entry.getValue() <= currentTime);
    }

    /**
     * Configuration class for rate limits
     */
    private static class RateLimitConfig {
        private final int maxRequests;
        private final int windowSeconds;

        public RateLimitConfig(int maxRequests, int windowSeconds) {
            this.maxRequests = maxRequests;
            this.windowSeconds = windowSeconds;
        }
    }

    /**
     * Close Redis connection on shutdown
     */
    public void shutdown() {
        scheduler.shutdown();
        connection.close();
        redisClient.shutdown();
    }
}
