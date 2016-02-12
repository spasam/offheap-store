package com.onshape.cache.buffer;

import java.nio.ByteBuffer;

/**
 * Composite byte buffer that is made up of an array of byte buffers.
 *
 * @author Seshu Pasam
 */
public class CompositeByteBuffer {
    final ByteBuffer[] buffers;

    CompositeByteBuffer(ByteBuffer[] buffers) {
        this.buffers = buffers;
    }

    /**
     * Transfer data from this to the specified byte buffer {@code dst}. This method can be called from multiple
     * threads, so it is synchronized.
     *
     * @param dst Destination byte buffer.
     */
    public synchronized void getBytes(ByteBuffer dst) {
        for (int i = 0; i < buffers.length; i++) {
            dst.put(buffers[i]);
            buffers[i].flip();
        }
    }
}
