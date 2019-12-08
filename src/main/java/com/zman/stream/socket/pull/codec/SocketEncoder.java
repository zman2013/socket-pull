package com.zman.stream.socket.pull.codec;

import com.zman.pull.stream.ISink;
import com.zman.pull.stream.bean.ReadResult;
import com.zman.pull.stream.bean.ReadResultEnum;
import com.zman.pull.stream.impl.DefaultThrough;
import com.zman.stream.socket.pull.EasyBuffer;

import java.nio.ByteBuffer;

/**
 * header: payload length (int: 4 bytes)
 * payload: content
 *
 * transform from byte[] to EasyBuffer
 */
public class SocketEncoder extends DefaultThrough<byte[], EasyBuffer> {

    private EasyBuffer easyBuffer = new EasyBuffer();

    @Override
    public ReadResult get(boolean end, Throwable throwable, ISink sink) {

        ReadResult readResult = source.get(end, throwable, sink);
        if(ReadResultEnum.Available.equals(readResult.status)){

            byte[] bytes = (byte[]) readResult.data;
            ByteBuffer byteBuffer = easyBuffer.getWritableByteBuffer();
            byteBuffer.putInt(bytes.length).put(bytes);

            readResult.data = easyBuffer;
        }

        return readResult;
    }
}
