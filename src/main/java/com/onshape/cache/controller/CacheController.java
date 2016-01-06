package com.onshape.cache.controller;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.onshape.cache.Cache;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.exception.EntryNotFoundException;
import com.onshape.cache.metrics.AbstractMetricsProvider;

@Controller
@RequestMapping("/")
public class CacheController extends AbstractMetricsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(CacheController.class);
    private static final String OCTET_STREAM = "application/octet-stream";

    @Autowired
    private Cache cache;

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ResponseStatus(value = HttpStatus.CREATED)
    public void create(@PathVariable("c") String c, @PathVariable("k") String k, HttpEntity<byte[]> value)
            throws CacheException {
        long start = System.currentTimeMillis();
        byte[] bytes = value.getBody();
        int size = bytes.length;
        LOG.info("Put: {}/{}: {} bytes", c, k, size);

        cache.put(c + k, bytes);

        int took = reportMetrics("cache.put", c, start);
        increment("cache.put.total.size.", c, size);
        increment("cache.put.total.time.", c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void get(HttpServletResponse response, @PathVariable("c") String c, @PathVariable("k") String k)
            throws CacheException, IOException {
        long start = System.currentTimeMillis();
        LOG.info("Get: {}/{}", c, k);

        ByteBuffer buffer = cache.get(c + k);
        if (buffer == null) {
            increment("cache.get.miss", c, 1);
            throw new EntryNotFoundException();
        }

        response.setStatus(HttpStatus.OK.value());
        response.setContentType(OCTET_STREAM);
        response.setContentLength(buffer.limit());

        WritableByteChannel channel = Channels.newChannel(response.getOutputStream());
        channel.write(buffer);

        int took = reportMetrics("cache.get", c, start);
        increment("cache.get.total.size.", c, buffer.limit());
        increment("cache.get.total.time.", c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.FOUND)
    public void contains(@PathVariable("c") String c, @PathVariable("k") String k) throws CacheException {
        LOG.info("Head: {}/{}", c, k);
        if (!cache.contains(c + k)) {
            increment("cache.head.miss", c, 1);
            throw new EntryNotFoundException();
        }
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void remove(@PathVariable("c") String c, @PathVariable("k") String k) throws CacheException {
        long start = System.currentTimeMillis();
        LOG.info("Delete: {}/{}", c, k);

        String key = c + k;
        if (!cache.contains(key)) {
            increment("cache.delete.miss", c, 1);
            throw new EntryNotFoundException();
        }

        cache.remove(key);
        reportMetrics("cache.delete", c, start);
    }
}
