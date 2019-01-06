package com.impossibl.postgres.protocol.io;

import java.io.IOException;
import java.net.SocketAddress;

public class StdHandlerContext extends AbstractHandlerContext implements IOOwningHandlerContext {

  private IOHandler handler;
  private IOLinkedHandlerContext next;
  private IOLinkedHandlerContext previous;

  StdHandlerContext(IOPipeline pipeline, IOHandler handler, IOLinkedHandlerContext next, IOLinkedHandlerContext previous) {
    super(pipeline);
    this.handler = handler;
    this.next = next;
    this.previous = previous;
  }

  @Override
  public IOHandler getHandler() {
    return handler;
  }

  @Override
  public void fireRegister() {
    try {
      handler.register(this);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
  }

  @Override
  public void fireDeregister() {
    try {
      handler.deregister(this);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
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
    return previous;
  }

  @Override
  public void setPrevious(IOLinkedHandlerContext previous) {
    this.previous = previous;
  }

  @Override
  public void connect(SocketAddress socketAddress) throws IOException {
    handler.connect(previous, socketAddress);
  }

  @Override
  public void disconnect() throws IOException {
    handler.disconnect(previous);
  }

  @Override
  public void write(Object msg) throws IOException {
    handler.write(previous, msg);
  }

  @Override
  public void flush() throws IOException {
    handler.flush(previous);
  }

  @Override
  public void fireOpen(){
    try {
      handler.open(next);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
  }

  @Override
  public void fireRead(Object msg){
    try {
      handler.read(next, msg);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
  }

  @Override
  public void fireReadComplete() {
    try {
      handler.readComplete(next);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
  }

  @Override
  public void fireClose() {
    try {
      handler.close(next);
    }
    catch (Throwable t) {
      fireExceptionCaught(t);
    }
  }

  @Override
  public void fireExceptionCaught(Throwable cause) {
    try {
      handler.exceptionCaught(next, cause);
    }
    catch (Throwable t) {
      // TODO what do we do? Ignore?
    }
  }

  @Override
  public String toString() {
    return "STD[" + handler.getClass().getSimpleName() + "]";
  }

}
