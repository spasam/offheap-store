package com.onshape;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CacheService {
    public static void main(String[] args) {
        SpringApplication.run(CacheService.class, args).setId("offheap-store");
    }
}
