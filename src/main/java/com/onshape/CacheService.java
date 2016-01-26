package com.onshape;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point into the Spring Boot application.
 *
 * @author Seshu Pasam
 */
@SpringBootApplication
public class CacheService {
    public static void main(String[] args) {
        SpringApplication.run(CacheService.class, args).setId("offheap-store");
    }
}
