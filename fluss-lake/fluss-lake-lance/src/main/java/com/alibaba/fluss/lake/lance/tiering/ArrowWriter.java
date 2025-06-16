package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.lance.utils.LanceArrowUtils;
import com.alibaba.fluss.lake.lance.writers.ArrowFieldWriter;
import com.alibaba.fluss.row.InternalRow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** An Arrow writer for InternalRow. */
public class ArrowWriter {
    /**
     * An array of writers which are responsible for the serialization of each column of the rows.
     */
    private final ArrowFieldWriter<InternalRow>[] fieldWriters;

    private int recordsCount;
    private VectorSchemaRoot root;

    public ArrowWriter(ArrowFieldWriter<InternalRow>[] fieldWriters, VectorSchemaRoot root) {
        this.fieldWriters = fieldWriters;
        this.root = root;
    }

    public static ArrowWriter create(VectorSchemaRoot root) {
        ArrowFieldWriter<InternalRow>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        for (int i = 0; i < fieldWriters.length; i++) {
            FieldVector fieldVector = root.getVector(i);

            fieldWriters[i] = LanceArrowUtils.createArrowFieldWriter(fieldVector);
        }
        return new ArrowWriter(fieldWriters, root);
    }

    /** Writes the specified row which is serialized into Arrow format. */
    public void writeRow(InternalRow row, int bucket, long offset) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i, true);
        }
        recordsCount++;
    }

    public void finish() {
        root.setRowCount(recordsCount);
    }
}
