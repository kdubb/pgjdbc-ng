package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

public class AbstractHandler implements IOHandler {

  @Override
  public void register(IOHandlerContext ctx) throws IOException {
  }

  @Override
  public void deregister(IOHandlerContext ctx) throws IOException {
  }

  @Override
  public void connect(IOHandlerContext ctx, SocketAddress socketAddress) throws IOException {
    ctx.connect(socketAddress);
  }

  @Override
  public void disconnect(IOHandlerContext ctx) throws IOException {
    ctx.disconnect();
  }

  @Override
  public void write(IOHandlerContext ctx, Object msg) throws IOException {
    ctx.write(msg);
  }

  @Override
  public void flush(IOHandlerContext ctx) throws IOException {
    ctx.flush();
  }

  @Override
  public void open(IOHandlerContext ctx) {
    ctx.fireOpen();
  }

  @Override
  public void read(IOHandlerContext ctx, Object msg) throws IOException {
    ctx.fireRead(msg);
  }

  @Override
  public void readComplete(IOHandlerContext ctx) throws IOException {
    ctx.fireReadComplete();
  }

  @Override
  public void close(IOHandlerContext ctx) throws IOException {
    ctx.fireClose();
  }

  @Override
  public void exceptionCaught(IOHandlerContext ctx, Throwable cause) {
    ctx.fireExceptionCaught(cause);
  }

}
