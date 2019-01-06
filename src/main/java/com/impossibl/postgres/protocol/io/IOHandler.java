package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

public interface IOHandler {

  void register(IOHandlerContext ctx) throws IOException;
  void deregister(IOHandlerContext ctx) throws IOException;

  void connect(IOHandlerContext ctx, SocketAddress socketAddress) throws IOException;

  void disconnect(IOHandlerContext ctx) throws IOException;

  void write(IOHandlerContext ctx, Object msg) throws IOException;

  void flush(IOHandlerContext ctx) throws IOException;

  void open(IOHandlerContext ctx) throws IOException;

  void read(IOHandlerContext ctx, Object msg) throws IOException;

  void readComplete(IOHandlerContext ctx) throws IOException;

  void close(IOHandlerContext ctx) throws IOException;

  void exceptionCaught(IOHandlerContext ctx, Throwable cause) throws IOException;

}
