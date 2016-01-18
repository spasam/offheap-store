package com.onshape.cache.buffer;

import java.nio.ByteBuffer;

public class CompositeByteBuffer {
    final ByteBuffer[] buffers;

    CompositeByteBuffer(ByteBuffer[] buffers) {
        this.buffers = buffers;
    }

    public void getBytes(ByteBuffer dst) {
        for (int i = 0; i < buffers.length; i++) {
            dst.put(buffers[i]);
            buffers[i].flip();
        }
    }

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
