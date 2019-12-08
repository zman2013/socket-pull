package com.zman.stream.socket.pull.codec;


import com.zman.pull.stream.ISink;
import com.zman.pull.stream.ISource;
import com.zman.pull.stream.impl.DefaultSink;
import com.zman.pull.stream.impl.DefaultSource;
import com.zman.stream.socket.pull.EasyBuffer;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.ws.Holder;
import java.nio.ByteBuffer;

import static com.zman.pull.stream.util.Pull.pull;

public class SocketEncoderTest {

    @Test
    public void encode(){

        ISource<byte[]> source = new DefaultSource<>();

        Holder<EasyBuffer> resultHolder = new Holder<>();
        ISink<EasyBuffer> sink = new DefaultSink<>(data->{resultHolder.value=data;return false;},()->{}, t->{});

        pull(source, new SocketEncoder(), sink);

        // source push int
        ByteBuffer sourceBuffer = ByteBuffer.allocate(4);
        sourceBuffer.putInt(Integer.MAX_VALUE).flip();
        source.push(sourceBuffer.array());

        // verify
        ByteBuffer resultBuffer = resultHolder.value.getReadableByteBuffer();
        Assert.assertEquals(4, resultBuffer.getInt());
        Assert.assertEquals(Integer.MAX_VALUE, resultBuffer.getInt());
    }

}
