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

    /**
     * 将byteBuffer所有未读的字节转化为字节数组
     * @return byte array
     */
    public byte[] toArray() {
        if (!flipped) {
            byteBuffer.flip();
            flipped = true;
        }
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * 清空byteBuffer，并写入数据
     * @param data
     */
    public void put(byte[] data) {

        if( data.length > 64*1024){
            throw new IllegalArgumentException("The length of the byte array should be less than 64k");
        }

        byteBuffer.clear();
        flipped = false;
        byteBuffer.put(data);
    }
}
