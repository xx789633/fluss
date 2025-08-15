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

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.types.RowType;

import com.google.common.collect.Sets;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;
import java.util.List;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<LogRecord> {
    private final Schema schema;
    private final Schema deleteSchema;
    private final RowDataWrapper wrapper;
    private final RowDataWrapper keyWrapper;
    private final boolean upsert = true;
    private final RowDataProjection keyProjection;

    BaseDeltaTaskWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<LogRecord> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema,
            RowType rowType,
            List<Integer> equalityFieldIds,
            int bucket) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
        this.wrapper = new RowDataWrapper(bucket, rowType, schema.asStruct());
        this.keyWrapper =
                new RowDataWrapper(
                        bucket,
                        (RowType)
                                TypeUtil.visit(deleteSchema, new IcebergDataTypeToFlussDataType()),
                        deleteSchema.asStruct());
        this.keyProjection =
                new RowDataProjection(rowType, schema.asStruct(), deleteSchema.asStruct());
    }

    abstract RowDataDeltaWriter route(LogRecord row);

    RowDataWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(LogRecord row) throws IOException {
        RowDataDeltaWriter writer = route(row);

        switch (row.getChangeType()) {
            case INSERT:
            case UPDATE_AFTER:
                if (upsert) {
                    writer.deleteKey(
                            new ProjectionLogRecord(
                                    row.logOffset(),
                                    row.timestamp(),
                                    row.getChangeType(),
                                    row.getRow(),
                                    this.keyProjection));
                }
                writer.write(row);
                break;

            default:
                throw new UnsupportedOperationException("Unknown row kind: " + row.getChangeType());
        }
    }

    protected class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
        RowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(LogRecord data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(LogRecord data) {
            return keyWrapper.wrap(data);
        }
    }
}
