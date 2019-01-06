package com.impossibl.postgres.protocol.io;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;

import io.netty.buffer.ByteBuf;

public class SocketHandler extends AbstractHandler {

  private Socket socket;
  private InputStream socketIn;
  private OutputStream socketOut;
  private Thread blockingThread;

  public Socket getSocket() {
    return socket;
  }

  public boolean isActive() {
    return socket != null && socket.isConnected();
  }

  @Override
  public void register(IOHandlerContext ctx) throws SocketException {
    socket = new Socket();
    socket.setTcpNoDelay(true);
    socket.setTrafficClass(0x10 | 0x08);
    socket.setSoTimeout(0);
  }

  @Override
  public void deregister(IOHandlerContext ctx) throws IOException {
    if (socket != null) {
      disconnect(ctx);
    }
  }

  @Override
  public void connect(IOHandlerContext ctx, SocketAddress socketAddress) throws IOException {
    socket.connect(socketAddress);
    socketIn = socket.getInputStream();
    socketOut = new BufferedOutputStream(socket.getOutputStream(), 16 * 1024);
    blockingThread = new BlockingReadThread("PGJDBC-NG I/O", ctx.getPipeline());
  }

  @Override
  public void disconnect(IOHandlerContext ctx) throws IOException {
    socket.close();
    socket = null;
    socketIn = null;
    socketOut = null;
    blockingThread.interrupt();
    blockingThread = null;
  }

  @Override
  public void close(IOHandlerContext ctx) throws IOException {
    if (socket != null) {
      disconnect(ctx);
    }
    super.close(ctx);
  }

  @Override
  public void write(IOHandlerContext ctx, Object msg) throws IOException {

    ByteBuf buf = (ByteBuf) msg;
    try {

      while (buf.isReadable()) {
        buf.readBytes(socketOut, buf.readableBytes());
      }

    }
    finally {
      buf.release();
    }
  }

  @Override
  public void flush(IOHandlerContext ctx) throws IOException {
    socketOut.flush();
  }

  class BlockingReadThread extends Thread {

    private IOInboundContext ctx;

    BlockingReadThread(String name, IOInboundContext ctx) {
      super(name);
      this.ctx = ctx;
      start();
      setPriority(MAX_PRIORITY);
    }

    public void run() {

      while (socket != null) {
        if (!socket.isConnected() || socket.isClosed()) {
          ctx.fireClose();
          return;
        }

        try {
          int amtRead;
          ByteBuf msg;
          do {
            msg = ctx.getAllocator().heapBuffer(8192);
            amtRead = socketIn.read(msg.array(), msg.arrayOffset(), msg.capacity());
            msg.writerIndex(amtRead);

            ctx.fireRead(msg);
          }
          while (amtRead == msg.capacity());

          ctx.fireReadComplete();
        }
        catch (EOFException e) {
          ctx.fireClose();
        }
        catch (SocketException e) {
          if (e.getMessage().equals("Socket closed")) {
            ctx.fireClose();
          }
          else {
            ctx.fireExceptionCaught(e);
          }
        }
        catch (Throwable cause) {
          ctx.fireExceptionCaught(cause);
        }
      }
    }

  }

}
