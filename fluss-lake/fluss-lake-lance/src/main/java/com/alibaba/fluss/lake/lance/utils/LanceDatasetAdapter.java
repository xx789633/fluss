package com.alibaba.fluss.lake.lance.utils;

import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.metadata.TableDescriptor;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.List;

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

    public static void appendFragments(LanceConfig config, List<FragmentMetadata> fragments) {
        FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
            Dataset.commit(
                            allocator,
                            config.getDatasetUri(),
                            appendOp,
                            java.util.Optional.of(datasetRead.version()),
                            options.getStorageOptions())
                    .close();
        }
    }
}
