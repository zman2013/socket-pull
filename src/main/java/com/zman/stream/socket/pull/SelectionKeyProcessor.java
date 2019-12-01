package com.zman.stream.socket.pull;

import com.zman.pull.stream.IDuplex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * OP_CONNECT/OP_READ/OP_WRITE事件处理器
 */
public class SelectionKeyProcessor {


    /**
     * SelectionKey的OP_CONNECT按位取反，用于不再订阅OP_CONNECT事件
     */
    private final static int OP_NOT_CONNECT = ~SelectionKey.OP_CONNECT;

    /**
     * client尝试连接server后，会调用此方法执行连接建立相关逻辑：
     * 1. 判断是否连接建立成功，无IO异常，基于channel建立duplex并回调外部函数方法
     * 2. 如果连接建立遇到IO异常，直接抛出异常
     * @param key                   selectionKey
     * @param selector              selector
     * @param onConnectedCallback   连接成功时的回调方法
     * @return  连接建立成功时，返回duplex
     * @throws IOException  io异常
     */
    SocketDuplex processConnect(SelectionKey key,
                                Selector selector,
                                Consumer<IDuplex> onConnectedCallback) throws IOException {
        key.interestOps(OP_NOT_CONNECT & key.interestOps());   // duplex has data, not interest OP_READ

        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {        // complete connection operation
            channel.finishConnect();
        }

        SocketDuplex duplex = new SocketDuplex(selector, channel);   // create duplex based on socket
        onConnectedCallback.accept(duplex);         // callback

        return duplex;
    }


    /**
     * SelectionKey的OP_READ按位取反，用于不再订阅OP_READ事件
     */
    private final static int OP_NOT_READ = ~SelectionKey.OP_READ;
    /**
     * 当socket可读时，会调用此方法：
     * 1. 从socket read buffer中读取数据最多64k
     * 2. 将数据转化为<code>byte array</code>后，push到duplex中
     * <p></p>
     *
     * OP_READ订阅，每次都是上游的sink从duplex中读取数据触发订阅的。
     * 所以每次selector中有OP_READ事件产生时，duplex一定是可写的
     *
     * @param duplex            duplex
     * @param socketChannel     socketChannel
     * @throws IOException      当从socket中读取数据遇到io异常时，直接抛出异常
     */
    void processReadable(SelectionKey key, SocketDuplex duplex, SocketChannel socketChannel) throws IOException {
        key.interestOps(OP_NOT_READ & key.interestOps());   // duplex has data, not interest OP_READ

        EasyBuffer easyBuffer = duplex.getSourceBuffer();
        ByteBuffer byteBuffer = easyBuffer.getWritableByteBuffer();

        socketChannel.read(byteBuffer);         // read from socket buffer

        byte[] bytes = easyBuffer.toArray();
        if( !duplex.push(bytes) ){  // push to duplex
            throw new RuntimeException("invoking duplex.push failed, this is unreachable");
        }
    }


    /**
     * SelectionKey的OP_WRITE按位取反，用于不再订阅OP_WRITE事件
     */
    private final static int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;

    /**
     * 当socket可写时，会调用此方法：
     * 1. 判断当前byteBuffer中是否有数据，如果有写入socket write buffer中
     * 2. 如果写完后byteBuffer为空，不再订阅OP_WRITE，并通过duplex从source读取数据
     * 3. 或者byteBuffer本来就是空，不再订阅OP_WRITE，并通过duplex从source读取数据
     * @param key               selectionKey
     * @param duplex            duplex
     * @param socketChannel     socketChannel
     * @throws IOException      如果向socket中写入数据遇到IO异常，直接抛出异常
     */
    void processWritable(SelectionKey key, SocketDuplex duplex, SocketChannel socketChannel) throws IOException {

        EasyBuffer easyBuffer = duplex.getSinkBuffer();
        ByteBuffer byteBuffer = easyBuffer.getReadableByteBuffer();

        int remaining = byteBuffer.remaining();
        if( remaining > 0 ){       // has elements
            int writeCount = socketChannel.write(byteBuffer);   // write to socket buffer
            if( writeCount == remaining ){
                key.interestOps(OP_NOT_WRITE & key.interestOps());  // byteBuffer is empty, won't interest OP_WRITE
                duplex.read();                                      // read from source
            }
        }else{
            key.interestOps(OP_NOT_WRITE & key.interestOps());
            duplex.read();
        }
    }

}
