package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

import io.netty.buffer.ByteBufAllocator;

public interface IOOutboundContext {

  ByteBufAllocator getAllocator();

  void connect(SocketAddress socketAddress) throws IOException;
  void disconnect() throws IOException;

  void write(Object msg) throws IOException;
  void flush() throws IOException;

  default void writeAndFlush(Object msg) throws IOException {
    write(msg);
    flush();
  }

}
