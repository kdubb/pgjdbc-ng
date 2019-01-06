package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class IOPipeline implements IOOutboundContext, IOInboundContext {

  private IOLinkedHandlerContext head;
  private IOLinkedHandlerContext tail;
  private ByteBufAllocator allocator = new PooledByteBufAllocator(false);

  public IOPipeline(IOHandler... handlers) {
    this.head = new HeadHandlerContext(this);
    this.tail = new TailHandlerContext(this);
    this.head.setNext(tail);
    this.tail.setPrevious(head);

    for (IOHandler handler : handlers) {
      appendHandler(handler);
    }
  }

  public ByteBufAllocator getAllocator() {
    return allocator;
  }

  public IOLinkedHandlerContext getHead() {
    return head;
  }

  public IOLinkedHandlerContext getTail() {
    return tail;
  }

  public boolean isActive() {
    SocketHandler socketHandler = (SocketHandler) findContext(SocketHandler.class).getHandler();
    return socketHandler.isActive();
  }

  public synchronized IOOwningHandlerContext findContext(Class<? extends IOHandler> handlerType) {
    IOLinkedHandlerContext context = head.getNext();
    while (context != null) {
      if (context instanceof IOOwningHandlerContext) {
        IOOwningHandlerContext owningHandlerContext = (IOOwningHandlerContext) context;
        if (handlerType.isInstance(owningHandlerContext.getHandler())) {
          return owningHandlerContext;
        }
      }

      context = context.getNext();
    }
    return null;
  }

  private void insertHandlerAfter(IOLinkedHandlerContext at, IOHandler handler) {
    StdHandlerContext handlerContext = new StdHandlerContext(this, handler, at.getNext(), at);
    at.getNext().setPrevious(handlerContext);
    at.setNext(handlerContext);

    handlerContext.fireRegister();
  }

  private void removeHandler(IOOwningHandlerContext at) {
    at.fireDeregister();

    at.getNext().setPrevious(at.getPrevious());
    at.getPrevious().setNext(at.getNext());
  }

  public synchronized void prependHandler(IOHandler handler) {
    insertHandlerAfter(head, handler);
  }

  public synchronized boolean prependHandler(IOHandler handler, Class<? extends IOHandler> before) {
    IOOwningHandlerContext beforeCtx = findContext(before);
    if (beforeCtx == null) return false;

    insertHandlerAfter(beforeCtx.getPrevious(), handler);

    return true;
  }

  public synchronized void appendHandler(IOHandler handler) {
    insertHandlerAfter(tail.getPrevious(), handler);
  }

  public synchronized boolean appendHandler(IOHandler handler, Class<? extends IOHandler> after) {
    IOOwningHandlerContext afterCtx = findContext(after);
    if (afterCtx == null) return false;

    insertHandlerAfter(afterCtx, handler);

    return true;
  }

  public synchronized boolean removeHandler(Class<? extends IOHandler> handlerType) {
    IOOwningHandlerContext ownerCtx = findContext(handlerType);
    if (ownerCtx == null) return false;

    removeHandler(ownerCtx);

    return true;
  }

  @Override
  public void connect(SocketAddress socketAddress) throws IOException {
    tail.connect(socketAddress);
  }

  @Override
  public void disconnect() throws IOException {
    tail.disconnect();
  }

  @Override
  public void write(Object msg) throws IOException {
    tail.write(msg);
  }

  @Override
  public void flush() throws IOException {
    tail.flush();
  }

  @Override
  public void writeAndFlush(Object msg) throws IOException {
    tail.writeAndFlush(msg);
  }

  @Override
  public void fireOpen() {
    head.fireOpen();
  }

  @Override
  public void fireRead(Object msg) {
    head.fireRead(msg);
  }

  @Override
  public void fireReadComplete() {
    head.fireReadComplete();
  }

  @Override
  public void fireClose() {
    head.fireClose();
  }

  @Override
  public void fireExceptionCaught(Throwable cause) {
    head.fireExceptionCaught(cause);
  }

}
