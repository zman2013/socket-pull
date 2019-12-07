package com.zman.stream.socket.pull;

import com.zman.pull.stream.IDuplexCallback;
import com.zman.pull.stream.ISink;
import com.zman.pull.stream.ISource;
import com.zman.pull.stream.bean.ReadResult;
import com.zman.pull.stream.bean.ReadResultEnum;
import com.zman.pull.stream.impl.DefaultDuplex;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class SocketDuplex extends DefaultDuplex<byte[]> {

    private Selector selector;

    private SocketChannel socketChannel;


    public SocketDuplex(Selector selector, SocketChannel socketChannel) {
        this.selector = selector;
        this.socketChannel = socketChannel;

        callback = new IDuplexCallback<byte[]>() {
            public void onClosed() {
                closeSocket();
            }
            public void onNext(byte[] data) {
                onData(data);
            }
            public void onError(Throwable throwable) {
                closeSocket();
            }
        };
    }

    private void closeSocket() {
        try {
            socketChannel.close();
        } catch (IOException e) {/* ignore */}
    }


    /**
     * read data from socket read buffer
     * @param data data
     */
    private void onData(byte[] data){
        sinkBuffer.put(data);
        try {
            interestOps(socketChannel, selector, SelectionKey.OP_WRITE);
        } catch (ClosedChannelException e) {
            callback.onError(e);
        }
    }


    @Override
    public ReadResult<byte[]> get(boolean end, ISink<byte[]> sink) {
        ReadResult<byte[]> readResult = super.get(end, sink);

        // source doesn't have more data, interest OP_READ
        if( ReadResultEnum.Waiting.equals(readResult.status) ){
            this.sink = sink;
            try {
                interestOps(socketChannel, selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                return new ReadResult<>(ReadResultEnum.Exception, e);
            }
        }

        return readResult;
    }

    /**
     * each time read one element, it won't read in loop.
     * instead this function will interest socket OP_WRITE.
     * @param source source stream
     */
    @Override
    public void read(ISource<byte[]> source) {
        this.source = source;

        ReadResult<byte[]> readResult = source.get(closed, this);

        switch (readResult.status){
            case Available:
                callback.onNext(readResult.data);
                break;
            case Waiting:
                callback.onWait();
                break;
            case Exception:
                callback.onError(readResult.throwable);
                closed = true;
                break;
            case End:
                callback.onClosed();
                closed = true;
                this.source = null;
        }
    }

    private void interestOps(SocketChannel socketChannel, Selector selector, int ops) throws ClosedChannelException {
        SelectionKey key = selector.keys().stream().findFirst().orElse(null);
        if(key==null){
            throw new RuntimeException("selector doesn't have a selectionKey, unreachable");
        }

        socketChannel.register(selector, key.interestOps()|ops);
    }


    private EasyBuffer sinkBuffer = new EasyBuffer();
    public EasyBuffer getSinkBuffer() { return sinkBuffer; }

    private EasyBuffer sourceBuffer = new EasyBuffer();
    public EasyBuffer getSourceBuffer() { return sourceBuffer; }

    public ISource<byte[]> source(){return source;}

}