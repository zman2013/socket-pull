package com.zman.stream.socket.pull;

import com.zman.pull.stream.IDuplexCallback;
import com.zman.pull.stream.ISink;
import com.zman.pull.stream.ISource;
import com.zman.pull.stream.bean.ReadResult;
import com.zman.pull.stream.bean.ReadResultEnum;
import com.zman.pull.stream.impl.DefaultDuplex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;


public class SocketDuplex extends DefaultDuplex<EasyBuffer> {

    private Selector selector;

    private SocketChannel socketChannel;


    public SocketDuplex(Selector selector, SocketChannel socketChannel) {
        this.selector = selector;
        this.socketChannel = socketChannel;

        callback = new IDuplexCallback<EasyBuffer>() {
            public void onClosed() {
                closeSocket();
            }
            public void onNext(EasyBuffer data) {
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
    private void onData(EasyBuffer data){
        sinkBuffer = data;
        try {
            interestOps(socketChannel, selector, SelectionKey.OP_WRITE);
        } catch (ClosedChannelException e) {
            callback.onError(e);
        }
    }


    @Override
    public ReadResult<EasyBuffer> get(boolean end, ISink<EasyBuffer> sink) {
        ReadResult<EasyBuffer> readResult = super.get(end, sink);

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
    public void read(ISource<EasyBuffer> source) {
        this.source = source;

        ReadResult<EasyBuffer> readResult = source.get(closed, this);

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


    private EasyBuffer sinkBuffer;
    public EasyBuffer getSinkBuffer() { return sinkBuffer; }

    private EasyBuffer sourceBuffer = new EasyBuffer();
    public EasyBuffer getSourceBuffer() { return sourceBuffer; }

    public ISource<EasyBuffer> source(){return source;}

}