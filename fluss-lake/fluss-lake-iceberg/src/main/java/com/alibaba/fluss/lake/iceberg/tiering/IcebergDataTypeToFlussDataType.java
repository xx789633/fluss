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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class IcebergDataTypeToFlussDataType extends TypeUtil.SchemaVisitor<DataType> {
    IcebergDataTypeToFlussDataType() {}

    @Override
    public DataType primitive(Type.PrimitiveType primitive) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return new BooleanType();
            case INTEGER:
                return new IntType();
            case LONG:
                return new BigIntType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case DATE:
                return new DateType();
            case TIME:
                // For the type: Flink only support TimeType with default precision (second) now.
                // The
                // precision of time is
                // not supported in Flink, so we can think of it as a simple time type directly.
                // For the data: Flink uses int that support mills to represent time data, so it
                // supports
                // mills precision.
                return new TimeType();
            case TIMESTAMP:
                Types.TimestampType timestamp = (Types.TimestampType) primitive;
                if (timestamp.shouldAdjustToUTC()) {
                    // MICROS
                    return new LocalZonedTimestampType(6);
                } else {
                    // MICROS
                    return new TimestampType(6);
                }
            case STRING:
                return new StringType();
            case UUID:
                // UUID length is 16
                return new BinaryType(16);
            case FIXED:
                Types.FixedType fixedType = (Types.FixedType) primitive;
                return new BinaryType(fixedType.length());
            case BINARY:
                return new BinaryType();
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return new DecimalType(decimal.precision(), decimal.scale());
            default:
                throw new UnsupportedOperationException(
                        "Cannot convert unknown type to Flink: " + primitive);
        }
    }
}
