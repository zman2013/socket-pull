package com.zman.stream.socket.pull;

import com.zman.net.pull.AbstractClient;
import com.zman.thread.eventloop.EventLoop;
import com.zman.thread.eventloop.impl.TaskType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.Callable;

public class SocketClient extends AbstractClient {

    private EventLoop eventLoop;

    private Selector selector;

    private SocketChannel socketChannel;

    private SocketDuplex duplex;

    private SelectionKeyProcessor keyProcessor;


    public SocketClient(EventLoop eventLoop) {
        this(eventLoop, new SelectionKeyProcessor());
    }

    public SocketClient(EventLoop eventLoop, SelectionKeyProcessor selectionKeyProcessor){
        this.eventLoop = eventLoop;
        this.keyProcessor = selectionKeyProcessor;
    }

    /**
     * 连接目标服务地址
     *
     * 1. 注册到selector
     * 2. 监听selector的事件
     * 3. 处理事件
     *
     * @param ip   ip
     * @param port 端口
     */
    @Override
    public void connect(String ip, int port) {
        try {
            selector = Selector.open();
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.register(selector, SelectionKey.OP_CONNECT);
            socketChannel.connect(new InetSocketAddress(ip, port));
        }catch (Throwable t){
            onThrowableCallback.accept(t);
            return;
        }

        eventLoop.submit(TaskType.IO.name(), ioAction);
    }

    /**
     * 关闭socket
     */
    @Override
    public void disconnect() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            onThrowableCallback.accept(e);
        }
    }

    private Callable<?> ioAction;
    {
        ioAction = () -> {
            try {
                if( !socketChannel.isOpen() ){       // channel is closed, invoke callback and exit event loop.
                    onDisconnectedCallback.run();
                    return null;
                }

                if (selector.select(10) > 0) {      // if has selection event
                    processSelectedKeys();
                }

            } catch (Throwable t) {
                onThrowableCallback.accept(t);
                try {
                    socketChannel.close();
                }catch (IOException ignored){}
            }

            eventLoop.submit(TaskType.IO.name(), ioAction);

            return null;
        };
    }

    /**
     * 处理selector上监听的事件：OP_CONNECT，OP_READ，OP_WRITE
     * @throws IOException  io异常
     */
    private void processSelectedKeys() throws IOException {
        Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

        while (keyIterator.hasNext()) {

            SelectionKey key = keyIterator.next();
            keyIterator.remove();

            if (key.isConnectable()) {         // connected to the server

                duplex = keyProcessor.processConnect(key, selector, onConnectedCallback);

            } else if (key.isReadable()) {     // io readable

                keyProcessor.processReadable(key, duplex, socketChannel);

            } else if (key.isWritable()) {     // io writable

                keyProcessor.processWritable(key, duplex, socketChannel);

            }
        }
    }

}
