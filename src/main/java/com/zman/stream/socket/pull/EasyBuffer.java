package com.zman.stream.socket.pull;

import java.nio.ByteBuffer;


public class EasyBuffer {

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(64 * 1024);

    private boolean flipped;

    /**
     * @return 可读的byteBuffer
     */
    public ByteBuffer getReadableByteBuffer() {
        if (!flipped) {
            byteBuffer.flip();
            flipped = true;
        }
        return byteBuffer;
    }

    /**
     * @return 可写的byteBuffer
     */
    public ByteBuffer getWritableByteBuffer() {
        byteBuffer.clear();
        flipped = false;
        return byteBuffer;
    }

}
