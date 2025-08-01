/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import org.apache.flink.annotation.Internal;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Time formats and timestamp formats respecting the RFC3339 specification, ISO-8601 specification
 * and SQL specification.
 */
@Internal
class TimeFormats {

    /** Formatter for RFC 3339-compliant string representation of a time value. */
    public static final DateTimeFormatter RFC3339_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .appendPattern("'Z'")
                    .toFormatter();

    /**
     * Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC
     * timezone).
     */
    public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(RFC3339_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /** Formatter for ISO8601 string representation of a timestamp value (with UTC timezone). */
    public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .appendPattern("'Z'")
                    .toFormatter();

    /** Formatter for SQL string representation of a time value. */
    public static final DateTimeFormatter SQL_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (with UTC timezone). */
    public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .appendPattern("'Z'")
                    .toFormatter();

    private TimeFormats() {}
}
