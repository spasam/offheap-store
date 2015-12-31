package com.onshape.cache.disk;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncConfig.class);

    @Value("${diskStoreCorePoolSize}")
    private int diskStoreCorePoolSize;
    @Value("${diskStoreMaxPoolSize}")
    private int diskStoreMaxPoolSize;

    @Override
    public Executor getAsyncExecutor() {
        LOG.info("Disk store core pool size: {}", diskStoreCorePoolSize);
        LOG.info("Disk store max pool size: {}", diskStoreMaxPoolSize);

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(diskStoreCorePoolSize);
        executor.setMaxPoolSize(diskStoreMaxPoolSize);
        executor.setThreadNamePrefix("ds-");
        executor.initialize();

        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AsyncUncaughtExceptionHandler() {
            @Override
            public void handleUncaughtException(Throwable ex, Method method, Object... params) {
                LOG.error("Error executing async task. Method: {}", method.getName(), ex);
            }
        };
    }
}
