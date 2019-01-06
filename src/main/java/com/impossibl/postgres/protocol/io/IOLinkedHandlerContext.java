package com.impossibl.postgres.protocol.io;

public interface IOLinkedHandlerContext extends IOHandlerContext {

  IOLinkedHandlerContext getNext();
  void setNext(IOLinkedHandlerContext next);

  IOLinkedHandlerContext getPrevious();
  void setPrevious(IOLinkedHandlerContext previous);

}
