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
import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

import java.util.ServiceLoader;

import io.netty.buffer.ByteBuf;


public class Procs {

  public static final Type.Codec.Decoder<CharSequence> DEFAULT_TEXT_DECODER = new Unknowns.TxtDecoder();
  public static final Type.Codec.Decoder<ByteBuf> DEFAULT_BINARY_DECODER = new Unknowns.BinDecoder();

  public static final Type.Codec.Encoder<StringBuilder> DEFAULT_TEXT_ENCODER = new Unknowns.TxtEncoder();
  public static final Type.Codec.Encoder<ByteBuf> DEFAULT_BINARY_ENCODER = new Unknowns.BinEncoder();

  public static final Modifiers.Parser DEFAULT_MOD_PARSER = new Unknowns.ModParser();

  private ServiceLoader<ProcProvider> providers;

  public Procs(ClassLoader classLoader) {
    try {
      providers = ServiceLoader.load(ProcProvider.class, classLoader);
    }
    catch (Exception e) {
      providers = ServiceLoader.load(ProcProvider.class, Procs.class.getClassLoader());
    }
  }

  public Type.TextCodec loadNamedTextCodec(String baseName, Context context) {
    return new Type.TextCodec(
        loadDecoderProc(baseName + "out", context, DEFAULT_TEXT_DECODER, CharSequence.class),
        loadEncoderProc(baseName + "in",  context, DEFAULT_TEXT_ENCODER, StringBuilder.class)
    );
  }

  public Type.BinaryCodec loadNamedBinaryCodec(String baseName, Context context) {
    return new Type.BinaryCodec(
        loadDecoderProc(baseName + "send", context, DEFAULT_BINARY_DECODER, ByteBuf.class),
        loadEncoderProc(baseName + "recv", context, DEFAULT_BINARY_ENCODER, ByteBuf.class)
    );
  }

  public <Buffer> Codec.Encoder<Buffer> loadEncoderProc(String name, Context context, Type.Codec.Encoder<Buffer> defaultEncoder, Class<? extends Buffer> bufferType) {

    if (!name.isEmpty()) {
      Codec.Encoder<Buffer> h;

      for (ProcProvider pp : providers) {
        if ((h = pp.findEncoder(name, context, bufferType)) != null)
          return h;
      }
    }

    return defaultEncoder;
  }

  public <Buffer> Codec.Decoder<Buffer> loadDecoderProc(String name, Context context, Type.Codec.Decoder<Buffer> defaultDecoder, Class<? extends Buffer> bufferType) {
    if (!name.isEmpty()) {
      Codec.Decoder<Buffer> h;

      for (ProcProvider pp : providers) {
        if ((h = pp.findDecoder(name, context, bufferType)) != null)
          return h;
      }
    }

    return defaultDecoder;
  }

  public Modifiers.Parser loadModifierParserProc(String name, Context context) {

    if (!name.isEmpty()) {
      Modifiers.Parser p;

      for (ProcProvider pp : providers) {
        if ((p = pp.findModifierParser(name, context)) != null)
          return p;
      }
    }

    return DEFAULT_MOD_PARSER;
  }

}
