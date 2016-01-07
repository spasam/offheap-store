package com.onshape.cache.controller;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.onshape.cache.Cache;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.exception.EntryNotFoundException;
import com.onshape.cache.metrics.MetricService;

@Validated
@Controller
@RequestMapping("/")
public class CacheController {
    private static final Logger LOG = LoggerFactory.getLogger(CacheController.class);
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final String VERSION_HEADER = "X-Version";
    private static final String CONTEXT_HEADER = "X-Context";

    @Autowired
    private Cache cache;
    @Autowired
    private MetricService ms;

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ResponseStatus(value = HttpStatus.CREATED)
    public void create(@NotNull @Size(min = 1) @PathVariable("c") String c,
            @NotNull @Size(min = 1) @RequestHeader(VERSION_HEADER) String v,
            @NotNull @Size(min = 1) @RequestHeader(CONTEXT_HEADER) String x,
            @NotNull @Size(min = 1) @PathVariable("k") String k,
            @NotNull @Size(min = 1) HttpEntity<byte[]> value)
                    throws CacheException {
        long start = System.currentTimeMillis();
        byte[] bytes = value.getBody();
        int size = bytes.length;
        LOG.info("Put: {}/{}/{}/{}: {} bytes", c, v, x, k, size);

        String key = c + "/" + v + "/" + x + "/" + k;
        cache.put(key, bytes);

        int took = ms.reportMetrics("put", c, start);
        ms.gauge("put.size." + c, size);
        ms.time("put.took." + c, took);
        ms.increment("put.total.size." + c, size);
        ms.increment("put.total.time." + c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void get(HttpServletResponse response,
            @NotNull @Size(min = 1) @PathVariable("c") String c,
            @NotNull @Size(min = 1) @RequestHeader(VERSION_HEADER) String v,
            @NotNull @Size(min = 1) @RequestHeader(CONTEXT_HEADER) String x,
            @NotNull @Size(min = 1) @PathVariable("k") String k)
                    throws CacheException, IOException {
        long start = System.currentTimeMillis();
        LOG.info("Get: {}/{}/{}/{}", c, v, x, k);

        String key = c + "/" + v + "/" + x + "/" + k;
        ByteBuffer buffer = cache.get(key);
        if (buffer == null) {
            ms.increment("get.miss");
            ms.increment("get.miss." + c);
            throw new EntryNotFoundException();
        }

        response.setStatus(HttpStatus.OK.value());
        response.setContentType(OCTET_STREAM);
        response.setContentLength(buffer.limit());

        WritableByteChannel channel = Channels.newChannel(response.getOutputStream());
        channel.write(buffer);

        int took = ms.reportMetrics("get", c, start);
        ms.gauge("get.size." + c, buffer.limit());
        ms.time("get.took." + c, took);
        ms.increment("get.total.size." + c, buffer.limit());
        ms.increment("get.total.time." + c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.FOUND)
    public void contains(@NotNull @Size(min = 1) @PathVariable("c") String c,
            @NotNull @Size(min = 1) @RequestHeader(VERSION_HEADER) String v,
            @NotNull @Size(min = 1) @RequestHeader(CONTEXT_HEADER) String x,
            @NotNull @Size(min = 1) @PathVariable("k") String k)
                    throws CacheException {
        LOG.info("Head: {}/{}/{}/{}", c, v, x, k);
        String key = c + "/" + v + "/" + x + "/" + k;
        if (!cache.contains(key)) {
            ms.increment("head.miss");
            ms.increment("head.miss." + c);
            throw new EntryNotFoundException();
        }
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void remove(@NotNull @Size(min = 1) @PathVariable("c") String c,
            @NotNull @Size(min = 1) @RequestHeader(VERSION_HEADER) String v,
            @NotNull @Size(min = 1) @RequestHeader(CONTEXT_HEADER) String x,
            @NotNull @Size(min = 1) @PathVariable("k") String k)
                    throws CacheException {
        long start = System.currentTimeMillis();
        LOG.info("Delete: {}/{}/{}/{}", c, v, x, k);

        String key = c + "/" + v + "/" + x + "/" + k;
        if (!cache.contains(key)) {
            ms.increment("delete.miss");
            ms.increment("delete.miss." + c);
            throw new EntryNotFoundException();
        }

        cache.remove(key);
        ms.reportMetrics("delete", c, start);
    }

    @RequestMapping(path = "{c}",
            method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeEntries(@NotNull @Size(min = 1) @PathVariable("c") String c,
            @NotNull @Size(min = 1) @RequestHeader(VERSION_HEADER) String v,
            @RequestHeader(CONTEXT_HEADER) String x)
                    throws CacheException {
        LOG.info("Delete entries: {}/{}/{}", c, v, x);
        if (StringUtils.isEmpty(x)) {
            cache.removeHierarchy(c + "/" + v);
        } else {
            cache.removeHierarchy(c + "/" + v + "/" + x);
        }
    }
}
