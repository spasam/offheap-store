package com.onshape.cache;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Base64Utils;

import com.onshape.CacheService;
import com.onshape.cache.exception.CacheException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(CacheService.class)
public class CacheTest {
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final int EXPIRES = 3600;

    @Autowired
    private Cache cache;
    @Autowired
    private OnHeap onHeap;
    @Autowired
    private OffHeap offHeap;
    @Autowired
    private DiskStore diskStore;

    @Test
    public void getPut() throws Exception {
        for (int kb : new int[] { 4, 16, 64, 256, 1024, 2048 }) {
            int size = kb * 1024;
            String key = getRandomKey();
            byte[] value = getRandomBytes(size);

            // Put is async for most part. So wait before getting
            cache.put(key, value, EXPIRES, true);
            Thread.sleep(500L);
            checkGet(key, value, size);

            // Remove from offHeap and try again
            offHeap.removeAsync(key);
            Thread.sleep(1000L);
            checkGet(key, value, size);

            // Remove from everywhere and try again
            Assert.assertTrue(onHeap.remove(key));
            offHeap.removeAsync(key);
            diskStore.removeAsync(key);
            Thread.sleep(2000L);
            checkBadKey(key);
        }
    }

    @Test
    public void remove() throws Exception {
        int size = 4 * 1024;
        String key = getRandomKey();
        byte[] value = getRandomBytes(size);

        // Put is async for most part. So wait before getting
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(500L);
        checkGet(key, value, size);

        // Remove is async. So wait and try to get bad key
        cache.remove(key);
        Thread.sleep(500L);
        checkBadKey(key);
    }

    @Test
    public void contains() throws Exception {
        int size = 4 * 1024;
        String key = getRandomKey();
        byte[] value = getRandomBytes(size);

        // Put is async for most part. So wait before checking
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(500L);
        checkGet(key, value, size);

        Assert.assertTrue("Key not found: " + key, cache.contains(key));

        // Remove from offHeap and try again
        offHeap.removeAsync(key);
        Thread.sleep(1000L);
        Assert.assertTrue("Key not found: " + key, cache.contains(key));

        // Remove everywhere and try again
        Assert.assertTrue(onHeap.remove(key));
        offHeap.removeAsync(key);
        diskStore.removeAsync(key);
        Thread.sleep(2000L);
        Assert.assertFalse("Key found: " + key, cache.contains(key));
        checkBadKey(key);
    }

    @Test
    public void replaceOnDisk() throws Exception {
        int size = (4 * 1024 * 1024) + 1;
        String key = getRandomKey();
        byte[] value = getRandomBytes(size);

        // Put is async for most part. So wait before checking
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(500L);
        checkGet(key, value, size);

        // Remove from offheap
        offHeap.removeAsync(key);
        Thread.sleep(1000L);
        checkGet(key, value, size);

        // Put a new value (smaller)
        size = 1024 * 1024;
        value = getRandomBytes(size);
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(1000L);
        checkGet(key, value, size);

        // Remove the new value from offheap
        offHeap.removeAsync(key);
        Thread.sleep(1000L);
        checkGet(key, value, size);

        // Put a new value (larger)
        size = (8 * 1024 * 1024) + 1;
        value = getRandomBytes(size);
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(1000L);
        checkGet(key, value, size);

        // Remove the new value from offheap
        offHeap.removeAsync(key);
        Thread.sleep(1000L);
        checkGet(key, value, size);
    }

    @Test
    public void replaceOffHeap() throws Exception {
        int size = 4 * 1024;
        String key = getRandomKey();
        byte[] value = getRandomBytes(size);

        // Put is async for most part. So wait before checking
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(500L);
        checkGet(key, value, size);

        // Put a new value (smaller)
        size = 1024;
        value = getRandomBytes(size);
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(1000L);
        checkGet(key, value, size);

        // Put a new value (larger)
        size = 8 * 1024;
        value = getRandomBytes(size);
        cache.put(key, value, EXPIRES, true);
        Thread.sleep(1000L);
        checkGet(key, value, size);
    }

    @Test
    public void removeHierarchy() throws Exception {
        for (String prefix : new String[] { "test/1", "test/1/x" }) {
            int size = 4 * 1024;
            String key = getRandomKey();
            byte[] value = getRandomBytes(size);

            // Put is async for most part. So wait before checking
            cache.put(key, value, EXPIRES, true);
            Thread.sleep(500L);
            checkGet(key, value, size);

            // Remove from onheap and offheap and use remove hierarchy
            Assert.assertTrue(onHeap.remove(key));
            offHeap.removeAsync(key);
            cache.removeHierarchy(prefix);
            Thread.sleep(2000L);

            checkBadKey(key);
        }
    }

    private void checkBadKey(String key) throws CacheException {
        ByteBuffer buffer = cache.get(key);
        Assert.assertNull("Unexpected entry for key: " + key, buffer);
    }

    private void checkGet(String key, byte[] expected, int size) throws CacheException {
        ByteBuffer buffer = cache.get(key);
        Assert.assertNotNull("Entry not found for key: " + key + ". Expected size: " + size, buffer);
        Assert.assertEquals("Expected position: 0. Got: " + buffer.position(), 0, buffer.position());
        Assert.assertEquals("Expected limit: " + size + ". Got: " + buffer.remaining(), size, buffer.remaining());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals("Byte mismatch at index: " + i, expected[i], buffer.get());
        }
    }

    private static String getRandomKey() {
        return "test/1/x/" + Base64Utils.encodeToUrlSafeString(getRandomBytes(32));
    }

    private static byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        RANDOM.nextBytes(bytes);

        return bytes;
    }
}
