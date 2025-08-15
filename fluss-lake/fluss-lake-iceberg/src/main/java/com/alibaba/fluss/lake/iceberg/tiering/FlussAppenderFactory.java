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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;

import java.io.Serializable;

public class FlussAppenderFactory implements FileAppenderFactory<LogRecord>, Serializable {
    @Override
    public FileAppender<LogRecord> newAppender(OutputFile outputFile, FileFormat fileFormat) {
        return null;
    }

    @Override
    public DataWriter<LogRecord> newDataWriter(
            EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return null;
    }

    @Override
    public EqualityDeleteWriter<LogRecord> newEqDeleteWriter(
            EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return null;
    }


    @Override
    public PositionDeleteWriter<LogRecord> newPosDeleteWriter(
            EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return null;
    }
}
