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
     * Transfer data from this to the specified byte buffer {@code dst}.
     *
     * @param dst Destination byte buffer.
     */
    public void getBytes(ByteBuffer dst) {
        for (int i = 0; i < buffers.length; i++) {
            dst.put(buffers[i]);
            buffers[i].flip();
        }
    }

    /**
     * Reads data from the provided byte array into this buffer.
     *
     * @param bytes Data to read from.
     */
    public void writeBytes(byte[] bytes) {
        int offset = 0;
        for (int i = 0; i < buffers.length; i++) {
            int length = Math.min(buffers[i].remaining(), (bytes.length - offset));
            buffers[i].put(bytes, offset, length);
            buffers[i].flip();
            offset += length;
        }
    }
}
