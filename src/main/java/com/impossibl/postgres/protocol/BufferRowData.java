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
package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.utils.ByteBufs.lengthEncode;

import java.io.IOException;

import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;


public class BufferRowData implements RowData {

  private ByteBuf buffer;
  private ResultField[] fields;
  private int[] fieldOffsets;

  private BufferRowData(ByteBuf buffer, ResultField[] fields, int[] fieldOffsets) {
    this.buffer = buffer;
    this.fields = fields;
    this.fieldOffsets = fieldOffsets;
  }

  public static BufferRowData encode(Context context, ResultField[] fields, Object[] values) throws IOException {

    ByteBuf fieldsBuffer = context.getProtocol().getChannel().alloc().buffer();

    int[] fieldOffsets = new int[fields.length];

    for (int fieldIdx = 0; fieldIdx < fields.length; ++fieldIdx) {

      ResultField field = fields[fieldIdx];
      Type fieldType = field.getTypeRef().get();
      Object value = values[fieldIdx];

      fieldOffsets[fieldIdx] =
          lengthEncode(fieldsBuffer, value, () -> {
            switch (field.getFormat()) {
              case Text: {
                StringBuilder fieldBuffer = new StringBuilder();
                fieldType.getTextCodec().getEncoder()
                    .encode(context, fieldType, value, null, fieldBuffer);

                ByteBufUtil.writeUtf8(fieldsBuffer, fieldBuffer);
              }
              break;

              case Binary: {
                fieldType.getBinaryCodec().getEncoder()
                    .encode(context, fieldType, value, null, fieldsBuffer);
              }
              break;
            }
          });
    }

    return new BufferRowData(fieldsBuffer, fields, fieldOffsets);
  }

  public static BufferRowData parseFields(ByteBuf buffer, ResultField[] fields) {

    int columnsCount = buffer.readUnsignedShort();
    int[] offsets = new int[columnsCount];

    for (int c = 0; c < columnsCount; ++c) {
      offsets[c] = buffer.readerIndex();
      buffer.skipBytes(max(buffer.readInt(), 0));
    }

    return new BufferRowData(buffer, fields, offsets);
  }

  @Override
  public int getColumnCount() {
    return fields.length;
  }

  @Override
  public Object getColumn(int columnIndex, Context context, Class<?> targetClass, Object targetContext) throws IOException {

    ResultField field = fields[columnIndex];
    Type type = field.getTypeRef().get();
    int offset = fieldOffsets[columnIndex];
    int length = buffer.getInt(offset);
    if (length == -1) {
      return null;
    }

    switch (field.getFormat()) {
      case Text: {
        Type.Codec.Decoder<CharSequence> decoder = type.getTextCodec().getDecoder();

        ByteBuf fieldBuffer = buffer.retainedSlice(offset + 4, length);
        try {
          String fieldString = fieldBuffer.toString(UTF_8);
          return decoder.decode(context, type, field.getTypeLength(), field.getTypeModifier(), fieldString, targetClass, targetContext);
        }
        finally {
          fieldBuffer.release();
        }
      }

      case Binary: {
        Type.Codec.Decoder<ByteBuf> decoder = type.getBinaryCodec().getDecoder();

        ByteBuf fieldBuffer = buffer.retainedSlice(offset + 4, length);
        try {
          return decoder.decode(context, type, field.getTypeLength(), field.getTypeModifier(), fieldBuffer, targetClass, targetContext);
        }
        finally {
          fieldBuffer.release();
        }
      }

      default:
        throw new IllegalStateException();
    }

  }

  @Override
  public BufferRowData retain() {
    ReferenceCountUtil.retain(buffer);
    return this;
  }

  @Override
  public void release() {
    ReferenceCountUtil.release(buffer);
  }

  @Override
  public void touch(Object hint) {
    ReferenceCountUtil.touch(buffer, hint);
  }

  @Override
  public UpdatableRowData duplicateForUpdate() {

    ByteBuf[] fieldBuffers = new ByteBuf[fieldOffsets.length];

    for (int fieldIndex = 0; fieldIndex < fieldOffsets.length; ++fieldIndex) {
      int fieldOffset = fieldOffsets[fieldIndex];
      int fieldLength = buffer.getInt(fieldOffset);
      if (fieldLength != -1) {
        ByteBuf fieldBuffer = buffer.alloc().buffer(fieldLength);
        buffer.getBytes(fieldOffset + 4, fieldBuffer, fieldLength);
        fieldBuffers[fieldIndex] = fieldBuffer;
      }
    }

    return new FieldBuffersRowData(fields, fieldBuffers);
  }

}
