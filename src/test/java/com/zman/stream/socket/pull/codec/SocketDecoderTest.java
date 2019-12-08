package com.zman.stream.socket.pull.codec;

import com.zman.pull.stream.ISink;
import com.zman.pull.stream.ISource;
import com.zman.pull.stream.impl.DefaultSink;
import com.zman.pull.stream.impl.DefaultSource;
import com.zman.stream.socket.pull.EasyBuffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.zman.pull.stream.util.Pull.pull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SocketDecoderTest {

    @Mock
    private Function<byte[],Boolean> onData;

    @Before
    public void before(){
        when(onData.apply(any())).thenReturn(false);
    }

    @Test
    public void decode(){

        ISource<EasyBuffer> source = new DefaultSource<>();

        ISink<byte[]> sink = new DefaultSink<>(onData,()->{},t->{});

        pull(source, new SocketDecoder(), sink);

        // prepare data
        EasyBuffer easyBuffer = new EasyBuffer();
        ByteBuffer buffer = easyBuffer.getWritableByteBuffer();
        buffer.putInt(4);
        buffer.putInt(Integer.MAX_VALUE);

        // source push data
        source.push(easyBuffer);

        // verify
        byte[] expected = new byte[4];
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(Integer.MAX_VALUE).flip();
        buf.get(expected);

        verify(onData, times(1)).apply(expected);

    }

    @Test
    public void decodeTwoPhase(){

        ISource<EasyBuffer> source = new DefaultSource<>();

        ISink<byte[]> sink = new DefaultSink<>(onData,()->{},t->{});

        pull(source, new SocketDecoder(), sink);

        // source push data length
        EasyBuffer easyBuffer = new EasyBuffer();
        ByteBuffer buffer = easyBuffer.getWritableByteBuffer();
        buffer.putInt(4);
        source.push(easyBuffer);

        // push data
        buffer = easyBuffer.getWritableByteBuffer();
        buffer.putInt(Integer.MAX_VALUE);
        source.push(easyBuffer);

        // verify
        byte[] expected = new byte[4];
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(Integer.MAX_VALUE).flip();
        buf.get(expected);

        verify(onData, times(1)).apply(expected);

    }

}
