package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.lance.testutils.FlinkLanceTieringTestBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;

/** IT case for tiering tables to lance. */
public class LanceTieringITCase extends FlinkLanceTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";
    private static StreamExecutionEnvironment execEnv;

    @BeforeAll
    protected static void beforeAll() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testTiering() throws Exception {
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // then, create another log table
        TablePath t2 = TablePath.of(DEFAULT_DB, "logTable");
        long t2Id = createLogTable(t2);
        TableBucket t2Bucket = new TableBucket(t2Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        // write records
        for (int i = 0; i < 10; i++) {
            rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
            flussRows.addAll(rows);
            // write records
            writeRows(t2, rows, true);
        }
        // check the status of replica after synced;
        // note: we can't update log start offset for unaware bucket mode log table
        assertReplicaStatus(t2Bucket, 30);

        // check data in paimon
        checkDataInLanceAppendOnlyTable(t2, flussRows, 0);
    }

    private void checkDataInLanceAppendOnlyTable(
            TablePath tablePath, List<InternalRow> expectedRows, long startingOffset)
            throws Exception {
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
    }
}
