package com.impossibl.postgres.protocol.io;

import io.netty.buffer.ByteBufAllocator;

public interface IOInboundContext {

  ByteBufAllocator getAllocator();

  void fireOpen();
  void fireRead(Object msg);
  void fireReadComplete();
  void fireClose();
  void fireExceptionCaught(Throwable cause);

}
