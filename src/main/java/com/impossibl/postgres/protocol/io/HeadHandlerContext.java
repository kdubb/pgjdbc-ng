package com.impossibl.postgres.protocol.io;

import java.net.SocketAddress;

class HeadHandlerContext extends AbstractHandlerContext implements IOLinkedHandlerContext {

  private IOLinkedHandlerContext next;

  HeadHandlerContext(IOPipeline pipeline) {
    super(pipeline);
  }

  @Override
  public IOLinkedHandlerContext getNext() {
    return next;
  }

  @Override
  public void setNext(IOLinkedHandlerContext next) {
    this.next = next;
  }

  @Override
  public IOLinkedHandlerContext getPrevious() {
    return null;
  }

  @Override
  public void setPrevious(IOLinkedHandlerContext previous) {
  }

  @Override
  public void connect(SocketAddress socketAddress) {
  }

  @Override
  public void disconnect() {
  }

  @Override
  public void write(Object msg) {
  }

  @Override
  public void flush() {
  }

  @Override
  public void fireOpen() {
    next.fireOpen();
  }

  @Override
  public void fireRead(Object msg) {
    next.fireRead(msg);
  }

  @Override
  public void fireReadComplete() {
    next.fireReadComplete();
  }

  @Override
  public void fireClose() {
    next.fireClose();
  }

  @Override
  public void fireExceptionCaught(Throwable cause) {
    next.fireExceptionCaught(cause);
  }

  @Override
  public String toString() {
    return "HEAD";
  }

}
