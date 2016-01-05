package com.onshape.cache.controller;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    @Qualifier("cache")
    @Autowired
    private Cache cache;

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @ResponseStatus(value = HttpStatus.CREATED)
    public void create(@PathVariable("c") String c, @PathVariable("k") String k, HttpEntity<byte[]> value)
            throws CacheException {
        byte[] bytes = value.getBody();
        int size = bytes.length;
        LOG.info("Put: {}/{}: {} bytes", c, k, size);

        long start = System.currentTimeMillis();
        cache.put(c + k, bytes);

        int took = reportMetrics("put", c, start);
        increment("put.total.size.", c, size);
        increment("put.total.time.", c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void get(HttpServletResponse response, @PathVariable("c") String c, @PathVariable("k") String k)
            throws CacheException, IOException {
        LOG.info("Get: {}/{}", c, k);

        long start = System.currentTimeMillis();
        ByteBuffer buffer = cache.get(c + k);
        if (buffer == null) {
            reportMetrics("get.miss", c, 0L);
            throw new EntryNotFoundException();
        }

        int took = reportMetrics("get", c, start);

        response.setStatus(HttpStatus.OK.value());
        response.setContentType(OCTET_STREAM);
        response.setContentLength(buffer.position());
        response.getOutputStream().write(buffer.array(), 0, buffer.position());

        increment("get.total.size.", c, buffer.position());
        increment("get.total.time.", c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.FOUND)
    public void exists(@PathVariable("c") String c, @PathVariable("k") String k) throws CacheException {
        LOG.info("Head: {}/{}", c, k);
        if (!cache.contains(c + k)) {
            reportMetrics("head.miss", c, 0L);
            throw new EntryNotFoundException();
        }
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void remove(@PathVariable("c") String c, @PathVariable("k") String k) throws CacheException {
        LOG.info("Delete: {}/{}", c, k);

        String key = c + k;
        if (!cache.contains(key)) {
            reportMetrics("delete.miss", c, 0L);
            throw new EntryNotFoundException();
        }

        long start = System.currentTimeMillis();
        cache.remove(key);
        reportMetrics("delete", c, start);
    }
}
