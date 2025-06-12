package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.metadata.TableDescriptor;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/** Lance dataset API adapter. */
public class LanceDatasetAdapter {
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public static void createDataset(
            String datasetUri, TableDescriptor tableDescriptor, WriteParams params) {
        Dataset.create(
                        allocator,
                        datasetUri,
                        LanceArrowUtils.toArrowSchema(tableDescriptor.getSchema().getRowType()),
                        params)
                .close();
    }
}
