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
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_NEGATIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.JAVA_DATE_POSITIVE_INFINITY_MSECS;
import static com.impossibl.postgres.system.procs.DatesTimes.NEG_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.POS_INFINITY;
import static com.impossibl.postgres.system.procs.DatesTimes.fromTimestampInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.timeJavaToPg;
import static com.impossibl.postgres.system.procs.DatesTimes.timePgToJava;
import static com.impossibl.postgres.system.procs.DatesTimes.timestampFromParsed;
import static com.impossibl.postgres.system.procs.DatesTimes.toDateInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.toTimeInTimeZone;
import static com.impossibl.postgres.system.procs.DatesTimes.toTimestampInTimeZone;
import static com.impossibl.postgres.types.PrimitiveType.TimestampTZ;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Calendar;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;

class Timestamps extends SettingSelectProcProvider {

  private PrimitiveType primitiveType;

  Timestamps(PrimitiveType primitiveType, String... baseNames) {
    super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
        null, null, null, null,
        null, null, null, null,
        baseNames);
    this.primitiveType = primitiveType;
    this.matchedBinEncoder = new BinEncoder();
    this.matchedBinDecoder = new BinDecoder();
    this.matchedTxtEncoder = this.unmatchedTxtEncoder = new TxtEncoder();
    this.matchedTxtDecoder = this.unmatchedTxtDecoder = new TxtDecoder();
  }

  private long microsecondsOf(long seconds, int nanoseconds) {

    long micros = SECONDS.toMicros(seconds);

    // Round nanoseconds to microseconds and add to total
    long insignificantNanoseconds = nanoseconds % 1000;
    if (insignificantNanoseconds >= 500) {
      nanoseconds += 1000 - insignificantNanoseconds;
    }

    micros += NANOSECONDS.toMicros(nanoseconds);

    return micros;
  }

  private long convertInput(Context context, Object value, Calendar calendar) throws ConversionException {

    if (value instanceof CharSequence) {
      CharSequence chars = (CharSequence) value;

      if (value.equals(POS_INFINITY)) return Long.MAX_VALUE;
      if (value.equals(NEG_INFINITY)) return Long.MIN_VALUE;

      TemporalAccessor parsed = context.getTimestampFormatter().getParser().parse(chars);
      ZonedDateTime dateTime;
      if (parsed.query(TemporalQueries.zone()) != null) {
        dateTime = ZonedDateTime.from(parsed);
      }
      else {
        dateTime = LocalDateTime.from(parsed).atZone(calendar.getTimeZone().toZoneId());
      }

      return microsecondsOf(dateTime.toEpochSecond(), dateTime.get(NANO_OF_SECOND));
    }

    if (value instanceof Timestamp) {
      Timestamp ts = (Timestamp) value;
      Instant instant = ts.toInstant();
      return microsecondsOf(instant.getEpochSecond(), instant.getNano());
    }

    if (value instanceof Time) {
      return toMicros(((Time) value).getTime());
    }

    if (value instanceof Date) {
      return toMicros(((Date) value).getTime());
    }

    throw new ConversionException(value.getClass(), primitiveType);
  }

  private Object convertOutput(Context context, long micros, Class<?> targetType, TimeZone targetTimeZone) throws ConversionException {

    if (targetType == Time.class) {
      long millis = toTimeInTimeZone(toMillis(micros), targetTimeZone);
      return new Time(millis);
    }

    if (targetType == Date.class) {
      long millis = toDateInTimeZone(toMillis(micros), targetTimeZone);
      return new Date(millis);
    }

    if (targetType == String.class) {
      if (micros == Long.MAX_VALUE) return POS_INFINITY;
      if (micros == Long.MIN_VALUE) return NEG_INFINITY;
      return context.getTimestampFormatter().getPrinter().formatMicros(micros, primitiveType == TimestampTZ ? context.getTimeZone() : targetTimeZone, primitiveType == TimestampTZ);
    }

    if (targetType == Timestamp.class) {

      if (micros == Long.MAX_VALUE) {
        return new Timestamp(JAVA_DATE_POSITIVE_INFINITY_MSECS);
      }
      else if (micros == Long.MIN_VALUE) {
        return new Timestamp(JAVA_DATE_NEGATIVE_INFINITY_MSECS);
      }

      long seconds = MICROSECONDS.toSeconds(micros);
      int nanoseconds = (int) MICROSECONDS.toNanos(micros - SECONDS.toMicros(seconds));
      if (nanoseconds < 0) {
        --seconds;
        nanoseconds += SECONDS.toNanos(1);
      }

      Timestamp timestamp = new Timestamp(SECONDS.toMillis(seconds));
      timestamp.setNanos(nanoseconds);

      return timestamp;
    }

    throw new ConversionException(primitiveType, targetType);
  }

  private long toMillis(long micros) {
    if (micros == Long.MAX_VALUE) return JAVA_DATE_POSITIVE_INFINITY_MSECS;
    if (micros == Long.MIN_VALUE) return JAVA_DATE_NEGATIVE_INFINITY_MSECS;
    return MICROSECONDS.toMillis(micros);
  }

  private long toMicros(long millis) {
    if (millis == JAVA_DATE_POSITIVE_INFINITY_MSECS) return Long.MAX_VALUE;
    if (millis == JAVA_DATE_NEGATIVE_INFINITY_MSECS) return Long.MIN_VALUE;
    return MILLISECONDS.toMicros(millis);
  }

  private class BinDecoder extends BaseBinaryDecoder {

    BinDecoder() {
      super(8);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return primitiveType;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Timestamp.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros = buffer.readLong();

      if (micros != Long.MAX_VALUE && micros != Long.MIN_VALUE) {

        micros = timePgToJava(micros, MICROSECONDS);

        long millis = MICROSECONDS.toMillis(micros);
        if (primitiveType != TimestampTZ) {
          millis = toTimestampInTimeZone(millis, calendar.getTimeZone());
        }

        micros = MILLISECONDS.toMicros(millis) + (micros - MILLISECONDS.toMicros(MICROSECONDS.toMillis(micros)));
      }

      return convertOutput(context, micros, targetClass, calendar.getTimeZone());
    }

  }

  private class BinEncoder extends BaseBinaryEncoder {

    BinEncoder() {
      super(8);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
      return primitiveType;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, ByteBuf buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long micros = convertInput(context, value, calendar);
      if (micros != Long.MAX_VALUE && micros != Long.MIN_VALUE) {

        micros = timeJavaToPg(micros, MICROSECONDS);

        long millis = MICROSECONDS.toMillis(micros);
        if (primitiveType != TimestampTZ) {
          millis = fromTimestampInTimeZone(millis, calendar.getTimeZone());
        }

        micros = MILLISECONDS.toMicros(millis) + (micros - MILLISECONDS.toMicros(MICROSECONDS.toMillis(micros)));
      }

      buffer.writeLong(micros);
    }

  }

  class TxtDecoder extends BaseTextDecoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return primitiveType;
    }

    @Override
    public Class<?> getDefaultClass() {
      return Timestamp.class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException {

      Calendar calendar = targetContext != null ? (Calendar) targetContext : Calendar.getInstance();

      long micros;
      if (buffer.equals(POS_INFINITY)) {
        micros = MILLISECONDS.toMicros(JAVA_DATE_POSITIVE_INFINITY_MSECS);
      }
      else if (buffer.equals(NEG_INFINITY)) {
        micros = MILLISECONDS.toMicros(JAVA_DATE_NEGATIVE_INFINITY_MSECS);
      }
      else {
        TemporalAccessor parsed = context.getTimestampFormatter().getParser().parse(buffer);

        TimeZone timeZone = primitiveType == TimestampTZ ? TimeZone.getTimeZone("UTC") : calendar.getTimeZone();

        micros = timestampFromParsed(parsed, timeZone);
      }

      return convertOutput(context, micros, targetClass, calendar.getTimeZone());
    }

  }

  class TxtEncoder extends BaseTextEncoder {

    @Override
    public PrimitiveType getPrimitiveType() {
      return primitiveType;
    }

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      Calendar calendar = sourceContext != null ? (Calendar) sourceContext : Calendar.getInstance();

      long micros = convertInput(context, value, calendar);
      if (micros == Long.MAX_VALUE) {
        buffer.append("infinity");
      }
      else if (micros == Long.MIN_VALUE) {
        buffer.append("-infinity");
      }
      else {

        String strVal = context.getTimestampFormatter().getPrinter().formatMicros(micros, calendar.getTimeZone(), primitiveType == TimestampTZ);

        buffer.append(strVal);
      }
    }

  }

}
