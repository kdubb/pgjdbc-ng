package com.impossibl.postgres.protocol.io;

public interface IOOwningHandlerContext extends IOLinkedHandlerContext {

  IOHandler getHandler();
  void fireRegister();
  void fireDeregister();

}
