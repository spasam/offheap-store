package com.onshape.cache.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.onshape.cache.Cache;
import com.onshape.cache.exception.InvalidValueException;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.exception.EntryNotFoundException;
import com.onshape.cache.metrics.MetricService;

/**
 * HTTP interface for cache service. Provides methods for CRUD operations over HTTP. For most of the methods below the
 * following arguments are used:
 *
 * <pre>
 * {@code c} is cache name (required)
 * {@code v} is cache version (required)
 * {@code x} cache context (optional)
 * {@code k} cache key (required)
 * </pre>
 *
 * @author Seshu Pasam
 */
@Validated
@Controller
@RequestMapping("/")
public class CacheController {
    private static final Logger LOG = LoggerFactory.getLogger(CacheController.class);
    private static final int TRANSFER_SIZE = 8192;
    private static final String HEADER_EXPIRES = "X-Expires";
    private static final String HEADER_USE_OFFHEAP = "X-UseOffHeap";

    @Autowired
    private Cache cache;
    @Autowired
    private MetricService ms;

    @RequestMapping(path = "{c}/{v}/{x}/{k:.+}",
                    method = RequestMethod.PUT)
    @ResponseStatus(value = HttpStatus.CREATED)
    public void create(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x,
                    @NotNull @Size(min = 1) @PathVariable("k") String k,
                    @Min(0) @RequestHeader(HEADER_EXPIRES) int expireSecs,
                    @NotNull @Size(min = 1) HttpEntity<byte[]> value)
                                    throws CacheException {
        create(c, c + "/" + v + "/" + x + "/" + k, value, expireSecs);
    }

    @RequestMapping(path = "{c}/{v}/{k:.+}",
                    method = RequestMethod.PUT)
    @ResponseStatus(value = HttpStatus.CREATED)
    public void create(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("k") String k,
                    @Min(0) @RequestHeader(HEADER_EXPIRES) int expireSecs,
                    @NotNull @Size(min = 1) HttpEntity<byte[]> value)
                                    throws CacheException {
        create(c, c + "/" + v + "/" + k, value, expireSecs);
    }

    private void create(String c, String key, HttpEntity<byte[]> value, int expireSecs) throws CacheException {
        long start = System.currentTimeMillis();
        byte[] bytes = value.getBody();
        if (bytes == null || bytes.length < 1) {
            LOG.warn("Not storing entry with invalid value: {}", key);
            throw new InvalidValueException("Invalid value for key: " + key);
        }

        int size = bytes.length;
        String useOffHeap = value.getHeaders().getFirst(HEADER_USE_OFFHEAP);

        cache.put(key, bytes, expireSecs, (useOffHeap == null || "true".equalsIgnoreCase(useOffHeap)));

        int took = ms.reportMetrics("put", c, start);
        ms.gauge("put.size." + c, size);
        ms.time("put.took." + c, took);
        ms.increment("put.total.size." + c, size);
        ms.increment("put.total.time." + c, took);
    }

    @RequestMapping(path = "{c}/{v}/{x}/{k:.+}",
                    method = RequestMethod.GET)
    public void get(HttpServletResponse response,
                    @NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException, IOException {
        get(response, c, c + "/" + v + "/" + x + "/" + k);
    }

    @RequestMapping(path = "{c}/{v}/{k:.+}",
                    method = RequestMethod.GET)
    public void get(HttpServletResponse response,
                    @NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException, IOException {
        get(response, c, c + "/" + v + "/" + k);
    }

    private void get(HttpServletResponse response, String c, String key) throws CacheException, IOException {
        long start = System.currentTimeMillis();

        ByteBuffer buffer = cache.get(key);
        if (buffer == null) {
            ms.increment("get.miss");
            ms.increment("get.miss." + c);
            throw new EntryNotFoundException();
        }

        int size = buffer.remaining();
        OutputStream os = response.getOutputStream();

        response.setStatus(HttpStatus.OK.value());
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setContentLength(size);

        int length;
        byte[] buf = new byte[TRANSFER_SIZE];
        while (buffer.remaining() > 0) {
            length = Math.min(buffer.remaining(), TRANSFER_SIZE);
            buffer.get(buf, 0, length);
            os.write(buf, 0, length);
        }

        int took = ms.reportMetrics("get", c, start);
        ms.gauge("get.size." + c, size);
        ms.time("get.took." + c, took);
        ms.increment("get.total.size." + c, size);
        ms.increment("get.total.time." + c, took);
    }

    @RequestMapping(path = "list/{c}/{v}/{x}",
                    method = RequestMethod.GET)
    public @ResponseBody List<String> list(HttpServletResponse response,
                    @NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x)
                                    throws CacheException, IOException {
        long start = System.currentTimeMillis();
        List<String> list = cache.list(c + "/" + v + "/" + x);
        ms.reportMetrics("list", c, start);

        return list;
    }

    @RequestMapping(path = "{c}/{v}/{x}/{k:.+}",
                    method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.OK)
    public void contains(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException {
        contains(c, c + "/" + v + "/" + x + "/" + k);
    }

    @RequestMapping(path = "{c}/{v}/{k:.+}",
                    method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.OK)
    public void contains(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException {
        contains(c, c + "/" + v + "/" + k);
    }

    private void contains(String c, String key) throws CacheException {
        if (!cache.contains(key)) {
            ms.increment("head.miss");
            ms.increment("head.miss." + c);
            throw new EntryNotFoundException();
        }
    }

    @RequestMapping(path = "{c}/{v}/{x}/{k:.+}",
                    method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void remove(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException {
        remove(c, c + "/" + v + "/" + x + "/" + k);
    }

    @RequestMapping(path = "{c}/{v}/{k:.+}",
                    method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void remove(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("k") String k)
                                    throws CacheException {
        remove(c, c + "/" + v + "/" + k);
    }

    private void remove(String c, String key) throws CacheException {
        long start = System.currentTimeMillis();
        if (!cache.contains(key)) {
            ms.increment("delete.miss");
            ms.increment("delete.miss." + c);
            throw new EntryNotFoundException();
        }

        cache.remove(key);
        ms.reportMetrics("delete", c, start);
    }

    @RequestMapping(path = "{c}/{v}/{x}",
                    method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void removeHierarchy(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v,
                    @NotNull @Size(min = 1) @PathVariable("x") String x)
                                    throws CacheException {
        removeHierarchy(c + "/" + v + "/" + x);
    }

    @RequestMapping(path = "{c}/{v}",
                    method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void removeHierarchy(@NotNull @Size(min = 1) @PathVariable("c") String c,
                    @NotNull @Size(min = 1) @PathVariable("v") String v)
                                    throws CacheException {
        removeHierarchy(c + "/" + v);
    }

    private void removeHierarchy(String prefix) throws CacheException {
        LOG.info("Delete entries: {}/{}/{}", prefix);
        cache.removeHierarchy(prefix);
    }

    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void cleanupExpired() {
        cache.cleanupExpired();
    }
}
