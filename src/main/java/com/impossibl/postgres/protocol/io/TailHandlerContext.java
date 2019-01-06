package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

class TailHandlerContext extends AbstractHandlerContext implements IOLinkedHandlerContext {

  private IOLinkedHandlerContext previous;

  TailHandlerContext(IOPipeline pipeline) {
    super(pipeline);
  }

  @Override
  public IOLinkedHandlerContext getNext() {
    return null;
  }

  @Override
  public void setNext(IOLinkedHandlerContext next) {
  }

  @Override
  public IOLinkedHandlerContext getPrevious() {
    return previous;
  }

  @Override
  public void setPrevious(IOLinkedHandlerContext previous) {
    this.previous = previous;
  }

  @Override
  public void connect(SocketAddress socketAddress) throws IOException {
    previous.connect(socketAddress);
  }

  @Override
  public void disconnect() throws IOException {
    previous.disconnect();
  }

  @Override
  public void write(Object msg) throws IOException {
    previous.write(msg);
  }

  @Override
  public void flush() throws IOException {
    previous.flush();
  }

  @Override
  public void fireOpen() {
  }

  @Override
  public void fireRead(Object msg) {
  }

  @Override
  public void fireReadComplete() {
  }

  @Override
  public void fireClose() {
  }

  @Override
  public void fireExceptionCaught(Throwable cause) {
  }

  @Override
  public String toString() {
    return "TAIL";
  }

}
