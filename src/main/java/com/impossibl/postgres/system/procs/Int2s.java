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
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.types.PrimitiveType.Int2;

import java.io.IOException;
import java.text.ParseException;

import io.netty.buffer.ByteBuf;

public class Int2s extends SimpleProcProvider {

  public Int2s() {
    super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "int2");
  }

  static class BinDecoder extends NumericBinaryDecoder<Short> {

    BinDecoder() {
      super(2, Number::toString);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Int2;
    }

    @Override
    public Class<Short> getDefaultClass() {
      return Short.class;
    }

    @Override
    protected Short decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {
      return buffer.readShort();
    }

  }

  static class BinEncoder extends NumericBinaryEncoder<Short> {

    BinEncoder() {
      super(2, Short::parseShort, val -> val ? (short) 1 : (short) 0, Number::shortValue);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Int2;
    }

    @Override
    public Class<Short> getDefaultClass() {
      return Short.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Short value, Object sourceContext, ByteBuf buffer) throws IOException {
      buffer.writeShort(value);
    }

  }

  static class TxtDecoder extends NumericTextDecoder<Short> {

    TxtDecoder() {
      super(Object::toString);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Int2;
    }

    @Override
    public Class<Short> getDefaultClass() {
      return Short.class;
    }

    @Override
    protected Short decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {
      return context.getIntegerFormatter().parse(buffer.toString()).shortValue();
    }

  }

  static class TxtEncoder extends NumericTextEncoder<Short> {

    TxtEncoder() {
      super(Short::parseShort, val -> val ? (short) 1 : (short) 0, Number::shortValue);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return Int2;
    }

    @Override
    public Class<Short> getDefaultClass() {
      return Short.class;
    }

    @Override
    protected void encodeNativeValue(Context context, Type type, Short value, Object sourceContext, StringBuilder buffer) throws IOException {
      buffer.append(value);
    }

  }

}
