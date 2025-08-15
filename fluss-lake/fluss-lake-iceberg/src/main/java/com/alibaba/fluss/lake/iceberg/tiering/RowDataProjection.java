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

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.RowType;

import com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

import java.util.Map;

public class RowDataProjection implements InternalRow {
    private final InternalRow.FieldGetter[] getters;
    private InternalRow internalRow;

    public RowDataProjection(
            RowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
        Map<Integer, Integer> fieldIdToPosition = Maps.newHashMap();
        for (int i = 0; i < rowStruct.fields().size(); i++) {
            fieldIdToPosition.put(rowStruct.fields().get(i).fieldId(), i);
        }

        this.getters = new InternalRow.FieldGetter[projectType.fields().size()];
        for (int i = 0; i < getters.length; i++) {
            Types.NestedField projectField = projectType.fields().get(i);
            Types.NestedField rowField = rowStruct.field(projectField.fieldId());

            getters[i] =
                    createFieldGetter(
                            rowType,
                            fieldIdToPosition.get(projectField.fieldId()),
                            rowField,
                            projectField);
        }
    }

    public InternalRow wrap(InternalRow row) {
        this.internalRow = row;
        return this;
    }

    private static InternalRow.FieldGetter createFieldGetter(
            RowType rowType,
            int position,
            Types.NestedField rowField,
            Types.NestedField projectField) {
        switch (projectField.type().typeId()) {
            default:
                return InternalRow.createFieldGetter(rowType.getTypeAt(position), position);
        }
    }

    private Object getValue(int pos) {
        return getters[pos].getFieldOrNull(internalRow);
    }

    @Override
    public int getFieldCount() {
        return 0;
    }

    @Override
    public boolean isNullAt(int pos) {
        return getValue(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) getValue(pos);
    }

    @Override
    public byte getByte(int pos) {
        return (byte) getValue(pos);
    }

    @Override
    public short getShort(int pos) {
        return (short) getValue(pos);
    }

    @Override
    public int getInt(int pos) {
        return 0;
    }

    @Override
    public long getLong(int pos) {
        return 0;
    }

    @Override
    public float getFloat(int pos) {
        return 0;
    }

    @Override
    public double getDouble(int pos) {
        return 0;
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return null;
    }

    @Override
    public BinaryString getString(int pos) {
        return null;
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return null;
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return null;
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return null;
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return new byte[0];
    }

    @Override
    public byte[] getBytes(int pos) {
        return new byte[0];
    }
}
