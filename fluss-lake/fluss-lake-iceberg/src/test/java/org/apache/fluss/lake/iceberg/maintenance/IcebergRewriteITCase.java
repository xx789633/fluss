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

import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;

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
        writeFullTypeRow(t1, 1, 3);
        waitUntilSnapshot(t1Id, 1, 0);

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            writeFullTypeRow(t1, 4, 6);
            waitUntilSnapshot(t1Id, 1, 1);
            assertReplicaStatus(t1Bucket, 6);

            writeFullTypeRow(t1, 7, 9);
            waitUntilSnapshot(t1Id, 1, 2);
            assertReplicaStatus(t1Bucket, 9);

            checkFileCountInIcebergTable(t1, 3);

            writeFullTypeRow(t1, 10, 12);
            waitUntilSnapshot(t1Id, 1, 3);
            assertReplicaStatus(t1Bucket, 12);

            checkFileCountInIcebergTable(t1, 2);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void writeFullTypeRow(TablePath tablePath, int from, int to) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            rows.add(
                    row(
                            true,
                            (byte) 100,
                            (short) 200,
                            i,
                            i + 400L,
                            500.1f,
                            600.0d,
                            "v1",
                            Decimal.fromUnscaledLong(900, 5, 2),
                            Decimal.fromBigDecimal(new BigDecimal(1000), 20, 0),
                            TimestampLtz.fromEpochMillis(1698235273400L),
                            TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                            TimestampNtz.fromMillis(1698235273501L),
                            TimestampNtz.fromMillis(1698235273501L, 8000),
                            new byte[] {5, 6, 7, 8},
                            TypeUtils.castFromString("2023-10-25", DataTypes.DATE()),
                            TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()),
                            BinaryString.fromString("abc"),
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
        }
        writeRows(tablePath, rows, false);
    }
}
