/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.protocol.io.IOHandlerContext;
import com.impossibl.postgres.protocol.io.IOPipeline;

import static com.impossibl.postgres.protocol.FieldFormat.Text;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncode;
import static com.impossibl.postgres.utils.ByteBufs.writeCString;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class ProtocolChannel {

  // Frontend messages
  private static final byte PASSWORD_MSG_ID = 'p';
  private static final byte FLUSH_MSG_ID = 'H';
  private static final byte TERMINATE_MSG_ID = 'X';
  private static final byte SYNC_MSG_ID = 'S';
  private static final byte QUERY_MSG_ID = 'Q';
  private static final byte PARSE_MSG_ID = 'P';
  private static final byte BIND_MSG_ID = 'B';
  private static final byte DESCRIBE_MSG_ID = 'D';
  private static final byte EXECUTE_MSG_ID = 'E';
  private static final byte CLOSE_MSG_ID = 'C';
  private static final byte FUNCTION_CALL_MSG_ID = 'F';

  private IOHandlerContext ctx;
  private Charset charset;

  public ProtocolChannel(IOHandlerContext ctx, Charset charset) {
    this.ctx = ctx;
    this.charset = charset;
  }

  IOPipeline getPipeline() {
    return ctx.getPipeline();
  }

  ProtocolChannel flush() throws IOException {
    ctx.flush();
    return this;
  }

  ProtocolChannel writeSSLRequest() throws IOException {

    ByteBuf msg = ctx.getAllocator().buffer();

    msg.writeInt(8);
    msg.writeInt(80877103);

    ctx.write(msg);

    return this;
  }

  ProtocolChannel writeStartup(int protocolMajorVersion, int protocolMinorVersion, Map<String, Object> params) throws IOException {

    ByteBuf msg = beginMessage((byte) 0);

    // Version
    msg.writeShort(protocolMajorVersion);
    msg.writeShort(protocolMinorVersion);

    // Name=Value pairs
    for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
      writeCString(msg, paramEntry.getKey(), charset);
      writeCString(msg, paramEntry.getValue().toString(), charset);
    }

    msg.writeByte(0);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writePassword(String password) throws IOException {

    ByteBuf msg = beginMessage(PASSWORD_MSG_ID);

    writeCString(msg, password, charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writePassword(ByteBuf password) throws IOException {

    ByteBuf msg = beginMessage(PASSWORD_MSG_ID);

    msg.writeBytes(password);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeSCM(byte code) throws IOException {

    ByteBuf msg = ctx.getAllocator().buffer(1);
    msg.writeByte(code);
    ctx.write(msg);

    return this;
  }

  ProtocolChannel writeQuery(String query) throws IOException {

    ByteBuf msg = beginMessage(QUERY_MSG_ID);

    writeCString(msg, query, charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeParse(String stmtName, String query, TypeRef[] paramTypes) throws IOException {

    ByteBuf msg = beginMessage(PARSE_MSG_ID);

    writeCString(msg, stmtName != null ? stmtName : "", charset);
    writeCString(msg, query, charset);

    msg.writeShort(paramTypes.length);
    for (TypeRef paramType : paramTypes) {
      int paramTypeOid = paramType != null ? paramType.getOid() : 0;
      msg.writeInt(paramTypeOid);
    }

    endMessage(msg);

    return this;
  }

  private boolean isAllText(FieldFormatRef[] fieldFormats) {
    return fieldFormats.length == 1 && fieldFormats[0].getFormat() == Text;
  }

  ProtocolChannel writeBind(String portalName, String stmtName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormats) throws IOException {

    byte[] portalNameBytes = nullToEmpty(portalName).getBytes(charset);
    byte[] stmtNameBytes = nullToEmpty(stmtName).getBytes(charset);

    ByteBuf msg = beginMessage(BIND_MSG_ID);

    writeCString(msg, portalNameBytes);
    writeCString(msg, stmtNameBytes);

    loadParams(msg, parameterFormats, parameterBuffers);

    //Set format for results fields
    if (resultFieldFormats == null || resultFieldFormats.length == 0) {
      //Request all binary
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else if (isAllText(resultFieldFormats)) {
      //Shortcut to all text
      msg.writeShort(0);
    }
    else if (!isAllText(resultFieldFormats)) {
      //Select result format for each
      msg.writeShort(resultFieldFormats.length);
      for (FieldFormatRef formatRef : resultFieldFormats) {
        msg.writeShort(formatRef.getFormat().ordinal());
      }
    }

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeDescribe(ServerObjectType target, String targetName) throws IOException {

    ByteBuf msg = beginMessage(DESCRIBE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeExecute(String portalName, int maxRows) throws IOException {

    ByteBuf msg = beginMessage(EXECUTE_MSG_ID);

    writeCString(msg, portalName != null ? portalName : "", charset);
    msg.writeInt(maxRows);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeFunctionCall(int functionId, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers) throws IOException {

    ByteBuf msg = beginMessage(FUNCTION_CALL_MSG_ID);

    msg.writeInt(functionId);

    loadParams(msg, parameterFormats, parameterBuffers);

    msg.writeShort(1);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeClose(ServerObjectType target, String targetName) throws IOException {

    ByteBuf msg = beginMessage(CLOSE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    return this;
  }

  ProtocolChannel writeFlush() throws IOException {

    writeMessage(FLUSH_MSG_ID);

    return this;
  }

  ProtocolChannel writeSync() throws IOException {

    writeMessage(SYNC_MSG_ID);

    return this;
  }

  void writeTerminate() throws IOException {

    ByteBuf msg = beginMessage(TERMINATE_MSG_ID);

    ctx.writeAndFlush(msg);
  }

  private void writeMessage(byte msgId) throws IOException {

    ByteBuf msg = ctx.getAllocator().buffer(5);

    msg.writeByte(msgId);
    msg.writeInt(4);

    ctx.write(msg);
  }

  private ByteBuf beginMessage(byte msgId) {

    ByteBuf msg = ctx.getAllocator().buffer();

    if (msgId != 0)
      msg.writeByte(msgId);

    msg.markWriterIndex();

    msg.writeInt(-1);

    return msg;
  }

  private void endMessage(ByteBuf msg) throws IOException {

    int endPos = msg.writerIndex();

    msg.resetWriterIndex();

    int begPos = msg.writerIndex();

    msg.setInt(begPos, endPos - begPos);

    msg.writerIndex(endPos);

    ctx.write(msg);
  }

  private void loadParams(ByteBuf msg, FieldFormatRef[] fieldFormats, ByteBuf[] paramBuffers) throws IOException {

    // Select format for parameters
    if (fieldFormats == null) {
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else {
      msg.writeShort(fieldFormats.length);
      for (FieldFormatRef paramFormatRef : fieldFormats) {
        paramFormatRef = paramFormatRef != null ? paramFormatRef : FieldFormat.Text;
        msg.writeShort(paramFormatRef.getFormat().ordinal());
      }
    }

    // Values for each parameter
    if (paramBuffers == null) {
      msg.writeShort(0);
    }
    else {
      msg.writeShort(paramBuffers.length);
      for (ByteBuf paramBuffer : paramBuffers) {
        lengthEncode(msg, paramBuffer, () -> {
          msg.writeBytes(paramBuffer);
          paramBuffer.resetReaderIndex();
        });
      }
    }

  }

}
