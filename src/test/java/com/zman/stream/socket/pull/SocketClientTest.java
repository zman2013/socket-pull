package com.zman.stream.socket.pull;

import com.zman.net.pull.netty.NettyServer;
import com.zman.pull.stream.IDuplex;
import com.zman.pull.stream.impl.DefaultSink;
import com.zman.pull.stream.impl.DefaultSource;
import com.zman.pull.stream.impl.DefaultThrough;
import com.zman.thread.eventloop.EventLoop;
import com.zman.thread.eventloop.impl.DefaultEventLoop;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.zman.pull.stream.util.Pull.pull;

@RunWith(MockitoJUnitRunner.class)
public class SocketClientTest {

    @Test
    public void testConnectAndDisconnect() throws InterruptedException {
        // start local server
        NettyServer nettyServer = new NettyServer();
        nettyServer.onAccept(duplex -> pull(duplex, duplex))
                .listen(8080);

        // start socket client
        EventLoop eventLoop = new DefaultEventLoop();
        SocketClient socketClient = new SocketClient(eventLoop);

        CountDownLatch connectCountDown = new CountDownLatch(1);
        CountDownLatch disconnectCountDown = new CountDownLatch(1);
        socketClient.onConnected(duplex -> connectCountDown.countDown())
                .onDisconnected(disconnectCountDown::countDown)
                .connect("localhost", 8080);

        boolean success = connectCountDown.await(3, TimeUnit.SECONDS);
        if( !success ){
            throw new RuntimeException("connect to server failed");
        }

        socketClient.disconnect();
        success = disconnectCountDown.await(3, TimeUnit.SECONDS);
        if( !success ){
            throw new RuntimeException("disconnect failed");
        }

        eventLoop.shutdown();
        nettyServer.close();
    }

    @Test
    public void testReadWrite() throws InterruptedException {
        // start local server
        NettyServer nettyServer = new NettyServer();
        nettyServer
                .onAccept(
                        duplex -> pull(duplex,
                                new DefaultThrough<ByteBuf, ByteBuf>(byteBuf->{
                                    byte[] tmp = new byte[byteBuf.readableBytes()];
                                    byteBuf.duplicate().readBytes(tmp);
                                    System.out.println("netty:"+new String(tmp, StandardCharsets.UTF_8));
                                    return byteBuf;                               }),
                                duplex))
                .listen(9080);

        // declare source and sink
        DefaultSource<EasyBuffer> source = new DefaultSource<>();
        for(int i = 0; i < 10; i ++){
            EasyBuffer easyBuffer = new EasyBuffer();
            ByteBuffer buffer = easyBuffer.getWritableByteBuffer();
            buffer.put(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            source.push(easyBuffer);
        }
        StringBuilder sb = new StringBuilder();
        DefaultSink<EasyBuffer> sink = new DefaultSink<>(easyBuffer -> {
            ByteBuffer buffer = easyBuffer.getReadableByteBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            sb.append(new String(bytes, StandardCharsets.UTF_8));
        });

        // start socket client
        EventLoop eventLoop = new DefaultEventLoop();
        SocketClient socketClient = new SocketClient(eventLoop);

        CountDownLatch connectCountDown = new CountDownLatch(1);
        CountDownLatch disconnectCountDown = new CountDownLatch(1);
        socketClient
                .onConnected(duplex -> {
                    pull(source, duplex, sink);
                    connectCountDown.countDown(); })
                .onDisconnected(disconnectCountDown::countDown)
                .connect("localhost", 9080);

        boolean success = connectCountDown.await(3, TimeUnit.SECONDS);
        if( !success ){
            throw new RuntimeException("connect to server failed");
        }

        Thread.sleep(1000);  // wait for sending and receiving data completion

        socketClient.disconnect();
        success = disconnectCountDown.await(3, TimeUnit.SECONDS);
        if( !success ){
            throw new RuntimeException("disconnect failed");
        }

        eventLoop.shutdown();
        nettyServer.close();

        Assert.assertEquals("0123456789", sb.toString());
    }

}
