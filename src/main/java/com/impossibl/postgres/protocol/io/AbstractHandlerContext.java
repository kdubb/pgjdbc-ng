package com.impossibl.postgres.protocol.io;

import io.netty.buffer.ByteBufAllocator;

public abstract class AbstractHandlerContext implements IOHandlerContext {

  private IOPipeline pipeline;

  public AbstractHandlerContext(IOPipeline pipeline) {
    this.pipeline = pipeline;
  }

  @Override
  public IOPipeline getPipeline() {
    return pipeline;
  }

  @Override
  public ByteBufAllocator getAllocator() {
    return pipeline.getAllocator();
  }

}
