package com.zman.stream.socket.pull;

import com.zman.net.pull.netty.NettyServer;
import com.zman.pull.stream.IDuplex;
import com.zman.pull.stream.ISink;
import com.zman.pull.stream.ISource;
import com.zman.pull.stream.bean.ReadResult;
import com.zman.pull.stream.impl.DefaultSink;
import com.zman.pull.stream.impl.DefaultSource;
import com.zman.pull.stream.impl.DefaultThrough;
import com.zman.thread.eventloop.EventLoop;
import com.zman.thread.eventloop.impl.DefaultEventLoop;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.zman.pull.stream.util.Pull.pull;

@RunWith(MockitoJUnitRunner.class)
public class SocketClientTest {

    @Test
    public void testConnectAndDisconnect() throws InterruptedException {
        // start local server
        NettyServer nettyServer = new NettyServer();
        nettyServer.onAccept((channelId,duplex) -> pull(duplex, duplex))
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
                        (channleId,duplex)-> pull(duplex,
                                new DefaultThrough<ByteBuf, ByteBuf>(byteBuf->{
                                    byte[] tmp = new byte[byteBuf.readableBytes()];
                                    byteBuf.duplicate().readBytes(tmp);
                                    System.out.println("netty:"+new String(tmp, StandardCharsets.UTF_8));
                                    return byteBuf;                               }),
                                duplex))
                .listen(9081);

        // declare source and sink
        DefaultSource<EasyBuffer> source = new DefaultSource<>();
        for(int i = 0; i < 10; i ++){
            EasyBuffer easyBuffer = new EasyBuffer();
            ByteBuffer buffer = easyBuffer.getWritableByteBuffer();
            buffer.put(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            source.push(easyBuffer);
        }
        StringBuilder sb = new StringBuilder();
        ISink<EasyBuffer> sink = new DefaultSink<>();
        sink.onNext(easyBuffer -> {
            ByteBuffer buffer = easyBuffer.getReadableByteBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            sb.append(new String(bytes, StandardCharsets.UTF_8));
            return false;
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
                .connect("localhost", 9081);

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


    @Test
    public void socketDuplexSourceBufferIsEmpty() throws InterruptedException {

        // source
        ISource<ByteBuf> source = new DefaultSource<>();
        for(int i = 0; i < 10; i ++){
            ByteBuf buffer = Unpooled.buffer(100);
            buffer.writeBytes(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            source.push(buffer);
//            Thread.sleep(100);
        }

        // start local server
        NettyServer nettyServer = new NettyServer();
        nettyServer
                .onAccept(
                        (channleId,duplex)->
                                pull(source,
                                        new DefaultThrough<ByteBuf, ByteBuf>(byteBuf -> {
                                            byte[] tmp = new byte[byteBuf.readableBytes()];
                                            byteBuf.duplicate().readBytes(tmp);
                                            System.out.println("netty:" + new String(tmp, StandardCharsets.UTF_8));
                                            return byteBuf;
                                        }),
                                        duplex))
                .listen(9080);

        // declare  sink
        StringBuilder sb = new StringBuilder();
        ISink<EasyBuffer> sink = new DefaultSink<>();
        sink.onNext(easyBuffer -> {
            ByteBuffer buffer = easyBuffer.getReadableByteBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            sb.append(new String(bytes, StandardCharsets.UTF_8));
            return false;
        });

        // start socket client
        EventLoop eventLoop = new DefaultEventLoop();
        SocketClient socketClient = new SocketClient(eventLoop);

        CountDownLatch connectCountDown = new CountDownLatch(1);
        CountDownLatch disconnectCountDown = new CountDownLatch(1);
        socketClient
                .onConnected(duplex -> {
                    pull(duplex,
//                            new DefaultThrough() {
//                                public ReadResult get(boolean end, Throwable throwable, ISink sink) {
//                                    ReadResult readResult = super.get(end, throwable, sink);
//                                    System.out.println("read from socketDuplex: " + readResult.status);
//                                    return readResult;
//                                }
//                            },
                            sink);
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
