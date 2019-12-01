package com.zman.stream.socket.pull;

import com.zman.pull.stream.impl.DefaultSink;
import com.zman.pull.stream.impl.DefaultSource;
import com.zman.thread.eventloop.EventLoop;
import com.zman.thread.eventloop.impl.DefaultEventLoop;
import com.zman.thread.eventloop.impl.TaskType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.zman.pull.stream.util.Pull.pull;

@Slf4j
public class SocketClientExample {

    @Test
    public void test() throws InterruptedException, IOException {
        EventLoop eventLoop = new DefaultEventLoop();

        DefaultSource<byte[]> source = new DefaultSource<>();
        DefaultSink<byte[]> sink = new DefaultSink<>(buf ->
               System.out.println("sink: " + new String(buf, StandardCharsets.UTF_8)));

        new SocketClient(eventLoop)
                .onConnected(duplex->pull(source, duplex, sink))
                .onDisconnected(()-> log.info("disconnected"))
                .onThrowable(Throwable::printStackTrace)
                .connect("localhost", 8081);

        for( int i = 0; i < 10; i ++ ) {
            byte[] bytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);

            eventLoop.submit(TaskType.IO.name(), ()->{
                log.info("source: " + new String(bytes, StandardCharsets.UTF_8));
                source.push(bytes);return null;});
        }

        System.in.read();

    }


}
