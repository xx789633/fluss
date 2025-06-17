/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.utils.types.Tuple2;

import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.vector.ipc.ArrowReader;
import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for tiering to Lance via {@link LanceLakeTieringFactory}. */
public class LanceTieringTest {
    private @TempDir File tempWarehouseDir;
    private LanceLakeTieringFactory lanceLakeTieringFactory;
    private Configuration configuration;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toString());
        lanceLakeTieringFactory = new LanceLakeTieringFactory(configuration);
    }

    private static Stream<Arguments> tieringWriteArgs() {
        return Stream.of(Arguments.of(false));
    }

    @ParameterizedTest
    @MethodSource("tieringWriteArgs")
    void testTieringWriteTable(boolean isPartitioned) throws Exception {
        int bucketNum = 3;
        TablePath tablePath = TablePath.of("lance", "logTable");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        createTable(tablePath, isPartitioned, null, config);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<LanceWriteResult> writeResultSerializer =
                lanceLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<LanceCommittable> committableSerializer =
                lanceLakeTieringFactory.getCommitableSerializer();

        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // should no any missing snapshot
            assertThat(lakeCommitter.getMissingLakeSnapshot(1L)).isNull();
        }

        Map<Tuple2<String, Integer>, List<LogRecord>> recordsByBucket = new HashMap<>();
        List<String> partitions =
                isPartitioned ? Arrays.asList("p1", "p2", "p3") : Collections.singletonList(null);
        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (String partition : partitions) {
                try (LakeWriter<LanceWriteResult> lakeWriter =
                        createLakeWriter(tablePath, bucket, partition)) {
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    Tuple2<List<LogRecord>, List<LogRecord>> writeAndExpectRecords =
                            genLogTableRecords(partition, bucket, 10);
                    List<LogRecord> writtenRecords = writeAndExpectRecords.f0;
                    List<LogRecord> expectRecords = writeAndExpectRecords.f1;
                    recordsByBucket.put(partitionBucket, expectRecords);
                    for (LogRecord logRecord : writtenRecords) {
                        lakeWriter.write(logRecord);
                    }
                    // serialize/deserialize writeResult
                    LanceWriteResult lanceWriteResult = lakeWriter.complete();
                    byte[] serialized = writeResultSerializer.serialize(lanceWriteResult);
                    lanceWriteResults.add(
                            writeResultSerializer.deserialize(
                                    writeResultSerializer.getVersion(), serialized));
                }
            }
        }

        // second, commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // serialize/deserialize committable
            LanceCommittable lanceCommittable = lakeCommitter.toCommitable(lanceWriteResults);
            byte[] serialized = committableSerializer.serialize(lanceCommittable);
            lanceCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot = lakeCommitter.commit(lanceCommittable);
            assertThat(snapshot).isEqualTo(1);
        }

        ArrowReader reader = LanceDatasetAdapter.getArrowReader(config);
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            int batchRows = readerRoot.getRowCount();
            for (int i = 0; i < batchRows; i++) {
                int value = (int) readerRoot.getVector("c1").getObject(i);
            }
        }

        // then, check data
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partition : partitions) {
                Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
            }
        }
    }

    private LakeCommitter<LanceWriteResult, LanceCommittable> createLakeCommitter(
            TablePath tablePath) throws IOException {
        return lanceLakeTieringFactory.createLakeCommitter(() -> tablePath);
    }

    private LakeWriter<LanceWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition) throws IOException {
        return lanceLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        // don't care about tableId & partitionId
                        return new TableBucket(0, 0L, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return partition;
                    }
                });
    }

    private Tuple2<List<LogRecord>, List<LogRecord>> genLogTableRecords(
            @Nullable String partition, int bucket, int numRecords) {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow;
            if (partition != null) {
                // Partitioned table: include partition field in data
                genericRow = new GenericRow(3); // c1, c2, c3(partition)
                genericRow.setField(0, i);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString(partition)); // partition field
            } else {
                // Non-partitioned table
                genericRow = new GenericRow(3);
                genericRow.setField(0, i);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString("bucket" + bucket));
            }
            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return Tuple2.of(logRecords, logRecords);
    }

    private void createTable(
            TablePath tablePath,
            boolean isPartitioned,
            @Nullable Integer numBuckets,
            LanceConfig config)
            throws Exception {

        Field c1 = new Field("c1", FieldType.nullable(new ArrowType.Int(4 * 8, true)), null);
        Field c2 = new Field("c2", FieldType.nullable(new ArrowType.Utf8()), null);
        Field c3 = new Field("c3", FieldType.nullable(new ArrowType.Utf8()), null);
        ArrayList<Field> fields = new ArrayList<>(Arrays.asList(c1, c2, c3));
        doCreateLanceTable(tablePath, fields, config);
    }

    private void doCreateLanceTable(TablePath tablePath, List<Field> fields, LanceConfig config)
            throws Exception {
        Field bucketCol =
                new Field(
                        BUCKET_COLUMN_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field offsetCol =
                new Field(
                        OFFSET_COLUMN_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field timestampCol =
                new Field(
                        TIMESTAMP_COLUMN_NAME,
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null);
        fields.addAll(Arrays.asList(bucketCol, offsetCol, timestampCol));
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(config.getDatasetUri(), new Schema(fields), params);
    }
}
