package com.jordimarcal.telemetry.gateway.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The only cross-origin caller of the gateway is the dashboard served by the
 * telemetry-hub on :8082. Anything else stays browser-blocked.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/v1/**")
                .allowedOrigins("http://localhost:8082")
                .allowedMethods("GET", "POST")
                .maxAge(3600);
    }
}
