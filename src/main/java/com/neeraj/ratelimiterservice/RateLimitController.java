package com.neeraj.ratelimiterservice;

import com.google.common.base.Strings;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class RateLimitController {

    private final RateLimiterService rateLimiterService;

    @Autowired
    public RateLimitController(RateLimiterService rateLimiterService) {
        this.rateLimiterService = rateLimiterService;
    }

    @GetMapping("/resource")
    public ResponseEntity<String>  getResource(HttpServletRequest request) {
        String clientIp = extractClientIp(request);

        if (!rateLimiterService.isAllowed(clientIp)) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body("Rate limit exceeded. Please try again later.");
        }
        return ResponseEntity.ok("Fetched Data from Resource Server " + System.currentTimeMillis());
    }

    private String extractClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (!Strings.isNullOrEmpty(xForwardedFor)) {
            // Get the first IP if there are multiple IPs in the header
            return xForwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
