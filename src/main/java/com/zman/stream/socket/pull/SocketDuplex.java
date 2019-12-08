package com.zman.stream.socket.pull;

import com.zman.pull.stream.impl.DefaultDuplex;

import java.io.IOException;
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

        sink().onClosed(throwable -> close(null))
                .onNext(this::onData)
                .onWait(()->{
                    try {
                        interestOps(socketChannel, selector, SelectionKey.OP_READ);
                    } catch (ClosedChannelException e) {
                        close(e);
                    }
                });


    }

    private void close(Throwable throwable) {
        try {
            socketChannel.close();
            source().close(throwable);
            sink().close(throwable);
        } catch (IOException e) {/* ignore */}
    }


    /**
     * read data from socket read buffer
     * @param data data
     */
    private Boolean onData(EasyBuffer data){
        sinkBuffer = data;
        try {
            interestOps(socketChannel, selector, SelectionKey.OP_WRITE);
        } catch (ClosedChannelException e) {
            close(e);
        }
        return true;    // won't continue reading
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

}