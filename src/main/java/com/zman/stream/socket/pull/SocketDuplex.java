package com.zman.stream.socket.pull;

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

public class SocketDuplex extends DefaultDuplex<byte[]> {

    private ISource<byte[]> source;

    private Selector selector;

    private SocketChannel socketChannel;

    public SocketDuplex(Selector selector, SocketChannel socketChannel) {
        this.selector = selector;
        this.socketChannel = socketChannel;
    }

    /**
     * 关闭流
     */
    @Override
    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * 返回一条数据
     *
     * @param end  控制source是否结束数据的生产
     * @param sink <code>ISink</code>的引用，当<code>ISource</code>没有数据可以提供时会保存sink的引用
     * @return 本次读取数据的结果：Available 获取到数据，Waiting 等待回调，End 结束
     */
    @Override
    public ReadResult<byte[]> get(boolean end, ISink<byte[]> sink) {
        if (end) {
            close();
            return ReadResult.Completed;
        }

        super.sink = sink;

        byte[] data = super.buffer.poll();
        if( data == null ){
            this.sink = sink;
            try {
                interestOps(socketChannel, selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                return new ReadResult<>(ReadResultEnum.Exception, e);
            }
            return ReadResult.Waiting;
        }else{
            return new ReadResult<>(data);
        }
    }





    @Override
    public void read(ISource<byte[]> source) {
        this.source = source;

        read();
    }

    public void read() {
        ReadResult<byte[]> readResult = source.get(false, this);
        switch (readResult.status) {
            case Available:
                callback.onNext(readResult.data);
                sinkBuffer.put(readResult.data);
                try {
                    interestOps(socketChannel, selector, SelectionKey.OP_WRITE);
                } catch (ClosedChannelException e) {
                    callback.onError(e);
                }
                break;
            case Waiting:
                break;
            case End:
                callback.onClosed();
                break;
            case Exception:
                callback.onError(readResult.throwable);
                break;
        }
    }

    /**
     * 当sink收到waiting之后，当Source再次有了数据后会调用sink的{@link #notifyAvailable}方法进行通知
     * sink收到callback后，可以立刻读取数据
     */
    @Override
    public void notifyAvailable() {
        read();
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

}