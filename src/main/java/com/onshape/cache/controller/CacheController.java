package com.onshape.cache.controller;

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
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.onshape.cache.Cache;
import com.onshape.cache.exception.CacheException;
import com.onshape.cache.exception.EntryNotFoundException;
import com.onshape.cache.metrics.CacheMetrics;

@Controller
@RequestMapping("/")
public class CacheController {
    private static final Logger LOG = LoggerFactory.getLogger(CacheController.class);

    @Qualifier("cache")
    @Autowired
    private Cache cache;
    @Autowired
    private CacheMetrics metrics;

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

        int took = metrics.report("put", c, start);
        metrics.increment("put.total.size.", c, size);
        metrics.increment("put.total.time.", c, took);
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody byte[] get(@PathVariable("c") String c, @PathVariable("k") String k)
            throws CacheException {
        LOG.info("Get: {}/{}", c, k);

        long start = System.currentTimeMillis();
        byte[] value = cache.get(c + k);
        if (value == null) {
            metrics.report("get.miss", c, 0L);
            throw new EntryNotFoundException();
        }

        int took = metrics.report("get", c, start);
        metrics.increment("get.total.size.", c, value.length);
        metrics.increment("get.total.time.", c, took);

        return value;
    }

    @RequestMapping(path = "{c}/{k}",
            method = RequestMethod.HEAD)
    @ResponseStatus(value = HttpStatus.FOUND)
    public void exists(@PathVariable("c") String c, @PathVariable("k") String k) throws CacheException {
        LOG.info("Head: {}/{}", c, k);
        if (!cache.contains(c + k)) {
            metrics.report("head.miss", c, 0L);
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
            metrics.report("delete.miss", c, 0L);
            throw new EntryNotFoundException();
        }

        long start = System.currentTimeMillis();
        cache.remove(key);
        metrics.report("delete", c, start);
    }
}
