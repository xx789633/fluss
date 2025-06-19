package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.GenericRow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static com.alibaba.fluss.record.ChangeType.APPEND_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for Lance Arrow Writer. */
public class LanceArrowWriterTest {
    @Test
    public void test() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field field =
                    new Field(
                            "column1",
                            FieldType.nullable(
                                    org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
                            null);
            Field bucketCol =
                    new Field(
                            BUCKET_COLUMN_NAME,
                            FieldType.nullable(new ArrowType.Int(32, true)),
                            null);
            Field offsetCol =
                    new Field(
                            OFFSET_COLUMN_NAME,
                            FieldType.nullable(new ArrowType.Int(64, true)),
                            null);
            Field timestampCol =
                    new Field(
                            TIMESTAMP_COLUMN_NAME,
                            FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                            null);
            Schema schema = new Schema(Arrays.asList(field, bucketCol, offsetCol, timestampCol));

            final int totalRows = 125;
            final int batchSize = 34;

            final LanceArrowWriter arrowWriter =
                    new LanceArrowWriter(allocator, schema, batchSize, new TableBucket(0, 0L, 0));
            AtomicInteger rowsWritten = new AtomicInteger(0);
            AtomicInteger rowsRead = new AtomicInteger(0);
            AtomicLong expectedBytesRead = new AtomicLong(0);

            Thread writerThread =
                    new Thread(
                            () -> {
                                try {
                                    for (int i = 0; i < totalRows; i++) {
                                        GenericRow genericRow = new GenericRow(1);
                                        genericRow.setField(0, rowsWritten.incrementAndGet());
                                        LogRecord logRecord =
                                                new GenericRecord(
                                                        i,
                                                        System.currentTimeMillis(),
                                                        APPEND_ONLY,
                                                        genericRow);

                                        arrowWriter.write(logRecord);
                                    }
                                    arrowWriter.setFinished();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    throw e;
                                }
                            });

            Thread readerThread =
                    new Thread(
                            () -> {
                                try {
                                    while (arrowWriter.loadNextBatch()) {
                                        VectorSchemaRoot root = arrowWriter.getVectorSchemaRoot();
                                        int rowCount = root.getRowCount();
                                        rowsRead.addAndGet(rowCount);
                                        try (ArrowRecordBatch recordBatch =
                                                new VectorUnloader(root).getRecordBatch()) {
                                            expectedBytesRead.addAndGet(
                                                    recordBatch.computeBodyLength());
                                        }
                                        for (int i = 0; i < rowCount; i++) {
                                            int value =
                                                    (int) root.getVector("column1").getObject(i);
                                            assertThat(value)
                                                    .isEqualTo(rowsRead.get() - rowCount + i + 1);
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });

            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();
            assertThat(totalRows).isEqualTo(rowsWritten.get());
            assertThat(totalRows).isEqualTo(rowsRead.get());
            arrowWriter.close();
        }
    }
}
