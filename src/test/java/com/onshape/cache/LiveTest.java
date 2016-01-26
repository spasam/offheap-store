package com.onshape.cache;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.onshape.CacheService;

@Ignore
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(CacheService.class)
public class LiveTest {
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final String URL_PREFIX = "http://localhost:9090/";
    private static final String URL_SUFFIX = "/1/x/abcdefghijklmnopqrstuvwxyz-";
    private static final MultiValueMap<String, String> HEADERS = new HttpHeaders();

    static {
        HEADERS.put("X-Expires", Arrays.asList("8640000"));
    }

    private RestTemplate restTemplate;
    private ExecutorService es;

    @Test
    public void test() throws Exception {
        HttpClient httpClient = HttpClientBuilder.create()
                        .setMaxConnTotal(100)
                        .setMaxConnPerRoute(100)
                        .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                        .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                        .build();

        restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(httpClient));

        int cacheIndex = 0;
        es = Executors.newFixedThreadPool(100);

        for (int size : new int[] { 1024, 4096, 65536, 262144, 524288, 1048576, 4194304, 16777216, 33554432 }) {
            es.submit(new PutTask((char) ('a' + cacheIndex++) + "", size));
        }

        Thread.sleep(1000000L);
        es.shutdown();
    }

    private class PutTask implements Runnable {
        private final String cacheName;
        private final AtomicInteger count;
        private final byte[] bytes;
        private long putMs;
        private long getMs;

        private PutTask(String cacheName, int sizeBytes) throws IOException {
            this.cacheName = cacheName;
            this.count = new AtomicInteger(0);
            this.bytes = new byte[sizeBytes];
            try (InputStream is = new FileInputStream("/dev/urandom")) {
                IOUtils.read(is, bytes, 0, sizeBytes);
            }
        }

        @Override
        public void run() {
            do {
                try {
                    HttpEntity<byte[]> entity = new HttpEntity<byte[]>(bytes, HEADERS);

                    long start = System.currentTimeMillis();
                    restTemplate.put(URL_PREFIX + cacheName + URL_SUFFIX + count.getAndIncrement(), entity);
                    putMs += (System.currentTimeMillis() - start);

                    start = System.currentTimeMillis();
                    List<Future<?>> futures = new ArrayList<>();
                    for (int i = 0; i < 9; i++) {
                        futures.add(es.submit(new GetTask(cacheName, count)));
                    }
                    for (int i = 0; i < 9; i++) {
                        futures.get(i).get();
                    }
                    getMs += (System.currentTimeMillis() - start);

                    if (count.get() % 10 == 0) {
                        System.out.println(cacheName + "[" + count.get() + "] Put: " + (putMs / count.get())
                                        + ". Get: " + (getMs / count.get()));
                    }
                } catch (Exception e) {
                    System.out.print(e);
                }
            } while (true);
        }
    }

    private class GetTask implements Runnable {
        private final String cacheName;
        private final AtomicInteger count;

        private GetTask(String cacheName, AtomicInteger count) {
            this.cacheName = cacheName;
            this.count = count;
        }

        @Override
        public void run() {
            try {
                restTemplate.getForEntity(URL_PREFIX + cacheName + URL_SUFFIX + RANDOM.nextInt(count.get()),
                                byte[].class);
            } catch (Exception e) {
                System.out.print(e);
            }
        }
    }
}
