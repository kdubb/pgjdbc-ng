package com.impossibl.postgres.protocol.io;

public interface IOHandlerContext extends IOOutboundContext, IOInboundContext {

  IOPipeline getPipeline();

}
