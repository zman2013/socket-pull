package com.zman.stream.socket.pull.codec;

import com.zman.pull.stream.ISink;
import com.zman.pull.stream.bean.ReadResult;
import com.zman.pull.stream.bean.ReadResultEnum;
import com.zman.pull.stream.impl.DefaultThrough;
import com.zman.stream.socket.pull.EasyBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * header: payload length (int: 4 bytes)
 * payload: content
 *
 * transform from EasyBuffer to byte[]
 */
public class SocketDecoder extends DefaultThrough<EasyBuffer, byte[]> {

    private Status status = Status.StartReadingHead;

    private ByteBuffer headBuffer = ByteBuffer.allocate(4);
    private ByteBuffer bodyBuffer;

    private int contentLength = -1;

    private BlockingQueue<byte[]> updateBuffer = new LinkedBlockingQueue<>();


    public ReadResult<byte[]> get(boolean end, ISink sink) {
        if(updateBuffer.size()>0){
            return new ReadResult<>(updateBuffer.poll());
        }

        ReadResult readResult = source.get(end, sink);
        if(ReadResultEnum.Available.equals(readResult.status)){
            EasyBuffer buffer = (EasyBuffer) readResult.data;

            ByteBuffer buf = buffer.getReadableByteBuffer();

            while(buf.hasRemaining()) {
                switch (status) {
                    case StartReadingHead:
                        if (buf.remaining() >= 4) {
                            contentLength = buf.getInt();

                            status = Status.StartReadingBody;
                        } else {
                            headBuffer.put(buf);

                            status = Status.ContinueReadingHead;
                        }
                        break;
                    case ContinueReadingHead:
                        int remainHeadLength = 4 - headBuffer.position();
                        if (buf.remaining() >= remainHeadLength) {
                            byte[] tmp = new byte[remainHeadLength];
                            buf.get(tmp);
                            headBuffer.put(tmp).flip();

                            contentLength = headBuffer.getInt();

                            status = Status.StartReadingBody;
                        }else{
                            headBuffer.put(buf);
                        }
                        break;
                    case StartReadingBody:
                        bodyBuffer = ByteBuffer.allocate(contentLength);
                        if( buf.remaining() >= contentLength){
                            byte[] tmp = new byte[contentLength];
                            buf.get(tmp);
                            updateBuffer.offer(tmp);

                            reset();
                        }else{
                            byte[] tmp = new byte[buf.remaining()];
                            buf.get(tmp);

                            bodyBuffer.put(tmp);

                            status = Status.ContinueReadingBody;
                        }
                        break;
                    case ContinueReadingBody:
                        if( buf.remaining() >= contentLength - bodyBuffer.position()){
                            byte[] tmp = new byte[contentLength-bodyBuffer.position()];
                            buf.get(tmp);
                            bodyBuffer.put(tmp).flip();

                            tmp = new byte[contentLength];
                            bodyBuffer.get(tmp);
                            updateBuffer.offer(tmp);

                            reset();
                        }else{
                            byte[] tmp = new byte[buf.remaining()];
                            buf.get(tmp);

                            bodyBuffer.put(tmp);
                        }
                        break;
                }
            }

            if(updateBuffer.size()>0){
                readResult = new ReadResult<>(updateBuffer.poll());
            }

        }

        return readResult;
    }

    private void reset(){
        headBuffer.clear();
        bodyBuffer.clear();
        status = Status.StartReadingHead;
    }


    enum Status{
        StartReadingHead,
        ContinueReadingHead,
        StartReadingBody,
        ContinueReadingBody
    }

}
