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
package com.impossibl.postgres.datetime;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.TimeZone;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ISOTimeFormat implements DateTimeFormat {

  private Parser parser = new Parser();
  private Printer printer = new Printer();

  @Override
  public Parser getParser() {
    return parser;
  }

  @Override
  public Printer getPrinter() {
    return printer;
  }


  static class Parser implements DateTimeFormat.Parser {

    private static final DateTimeFormatter PARSER =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:mm", "+00")
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .appendPattern("GG")
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT)
            .withChronology(IsoChronology.INSTANCE);

    @Override
    public TemporalAccessor parse(CharSequence time) {
      return PARSER.parse(time);
    }

  }

  static class Printer implements DateTimeFormat.Printer {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final DateTimeFormatter PRINTER_WITH_TZ =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_TIME)
            .optionalStart()
            .appendOffset("+HH:mm", "+00")
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT)
            .withChronology(IsoChronology.INSTANCE);

    private static final DateTimeFormatter PRINTER_WITHOUT_TZ =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_TIME)
            .toFormatter()
            .withResolverStyle(ResolverStyle.LENIENT)
            .withChronology(IsoChronology.INSTANCE);

    @Override
    public String formatMillis(long millis, TimeZone timeZone, boolean displayTimeZone) {
      return formatMicros(MILLISECONDS.toMicros(millis), timeZone, displayTimeZone);
    }

    @Override
    public String formatMicros(long micros, TimeZone timeZone, boolean displayTimeZone) {

      timeZone = timeZone != null ? timeZone : UTC;

      ZoneOffset offset = ZoneOffset.ofTotalSeconds((int) MILLISECONDS.toSeconds(timeZone.getRawOffset()));
      OffsetTime time = OffsetTime.of(LocalTime.ofNanoOfDay(MICROSECONDS.toNanos(micros)), ZoneOffset.UTC).withOffsetSameInstant(offset);

      return (displayTimeZone ? PRINTER_WITH_TZ : PRINTER_WITHOUT_TZ).format(time);
    }

  }

}
