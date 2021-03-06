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
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.ServerObjectType.Statement;
import static com.impossibl.postgres.system.Empty.EMPTY_FIELDS;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class PrepareCommandImpl extends CommandImpl implements PrepareCommand {

  private String statementName;
  private String query;
  private Type[] parseParameterTypes;
  private TypeRef[] describedParameterTypes;
  private ResultField[] describedResultFields;
  private ProtocolListener listener = new BaseProtocolListener() {

    @Override
    public void parseComplete() {
    }

    @Override
    public boolean isComplete() {
      return describedResultFields != null || error != null || exception != null;
    }

    @Override
    public void parametersDescription(TypeRef[] parameterTypes) {
      PrepareCommandImpl.this.describedParameterTypes = parameterTypes;
    }

    @Override
    public void rowDescription(ResultField[] resultFields) {

      // Ensure we are working with binary fields
      for (ResultField field : resultFields)
        field.setFormat(FieldFormat.Binary);

      PrepareCommandImpl.this.describedResultFields = resultFields;
    }

    @Override
    public void noData() {
      PrepareCommandImpl.this.describedResultFields = EMPTY_FIELDS;
    }

    @Override
    public synchronized void error(Notice error) {
      PrepareCommandImpl.this.error = error;
      notifyAll();
    }

    @Override
    public synchronized void exception(Throwable cause) {
      setException(cause);
      notifyAll();
    }

    @Override
    public synchronized void ready(TransactionStatus txStatus) {
      notifyAll();
    }

    @Override
    public void notice(Notice notice) {
      addNotice(notice);
    }

  };

  PrepareCommandImpl(String statementName, String query, Type[] parseParameterTypes) {
    this.statementName = statementName;
    this.query = query;
    this.parseParameterTypes = parseParameterTypes;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public String getStatementName() {
    return statementName;
  }

  @Override
  public Type[] getParseParameterTypes() {
    return parseParameterTypes;
  }

  @Override
  public Type[] getDescribedParameterTypes() {
    Type[] types = new Type[describedParameterTypes.length];
    for (int typeIdx = 0; typeIdx < types.length; ++typeIdx) {
      types[typeIdx] = describedParameterTypes[typeIdx].get();
    }
    return types;
  }

  @Override
  public ResultField[] getDescribedResultFields() {
    return describedResultFields;
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    protocol.setListener(listener);

    ByteBuf msg = protocol.getChannel().alloc().buffer();

    protocol.writeParse(msg, statementName, query, parseParameterTypes);

    protocol.writeDescribe(msg, Statement, statementName);

    protocol.writeSync(msg);

    protocol.send(msg);

    listener.waitUntilComplete(networkTimeout);

  }

}
