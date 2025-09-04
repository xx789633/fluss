/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.TypeUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test to for Iceberg compaction. */
class IcebergRewriteITCase extends FlinkIcebergTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testCompaction() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createPkTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows =
                Arrays.asList(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                1,
                                1 + 400L,
                                500.1f,
                                600.0d,
                                "v1",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                2,
                                2 + 400L,
                                500.1f,
                                600.0d,
                                "v2",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                3,
                                3 + 400L,
                                500.1f,
                                600.0d,
                                "v3",
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                BinaryString.fromString("abc"),
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
        writeRows(t1, rows, false);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            checkFileInIcebergTable(t1, 1);

            // write another file
            rows =
                    Arrays.asList(
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    4,
                                    4 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v1",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    5,
                                    5 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v2",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    6,
                                    6 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v3",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
            writeRows(t1, rows, false);

            waitUntilSnapshot(t1Id, 1, 1);

            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 6);

            // write another file
            rows =
                    Arrays.asList(
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    7,
                                    7 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v1",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    8,
                                    8 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v2",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                            row(
                                    true,
                                    (byte) 100,
                                    (short) 200,
                                    9,
                                    9 + 400L,
                                    500.1f,
                                    600.0d,
                                    "v3",
                                    Decimal.fromUnscaledLong(900, 5, 2),
                                    Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                    TimestampLtz.fromEpochMillis(1698235273400L),
                                    TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                    TimestampNtz.fromMillis(1698235273501L),
                                    TimestampNtz.fromMillis(1698235273501L, 8000),
                                    new byte[] {5, 6, 7, 8},
                                    TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                                    TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                                    BinaryString.fromString("abc"),
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
            writeRows(t1, rows, false);

            waitUntilSnapshot(t1Id, 1, 2);

            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 9);

            checkFileInIcebergTable(t1, 3);
        } finally {
            jobClient.cancel().get();
        }
    }
}
