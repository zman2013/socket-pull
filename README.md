[![Travis Build](https://api.travis-ci.org/zman2013/socket-pull.svg?branch=master)](https://api.travis-ci.org/zman2013/socket-pull.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/zman2013/socket-pull/badge.svg?branch=master)](https://coveralls.io/github/zman2013/socket-pull?branch=master)


# netty-pull
a socket pull-stream implementation based on java nio.

## dependency
```xml
<dependency>
    <groupId>com.zmannotes</groupId>
    <artifactId>socket-pull</artifactId>
    <version>0.0.1</version>
</dependency>
```

## example

### Client
```java
new SocketClient(eventLoop)
        .onConnected(duplex->pull(source, duplex, sink))
        .onDisconnected(()-> log.info("disconnected"))
        .onThrowable(Throwable::printStackTrace)
        .connect("localhost", 8081);
```