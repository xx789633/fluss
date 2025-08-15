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

import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;

public class ProjectionLogRecord implements LogRecord {
    private final ChangeType changeType;
    private final RowDataProjection rowDataProjection;
    private final long timestamp;
    private final InternalRow internalRow;
    private final long logOffset;

    ProjectionLogRecord(
            long logOffset,
            long timestamp,
            ChangeType changeType,
            InternalRow internalRow,
            RowDataProjection rowDataProjection) {
        this.changeType = changeType;
        this.rowDataProjection = rowDataProjection;
        this.timestamp = timestamp;
        this.internalRow = internalRow;
        this.logOffset = logOffset;
    }

    @Override
    public long logOffset() {
        return this.logOffset;
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        return this.changeType;
    }

    @Override
    public InternalRow getRow() {
        return rowDataProjection.wrap(this.internalRow);
    }
}
