package com.alibaba.fluss.lake.lance.utils;

import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.tiering.LanceArrowWriter;
import com.alibaba.fluss.metadata.TableBucket;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.ipc.ColumnOrdering;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Optional;

/** Lance dataset API adapter. */
public class LanceDatasetAdapter {
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public static void createDataset(String datasetUri, Schema schema, WriteParams params) {
        Dataset.create(allocator, datasetUri, schema, params).close();
    }

    public static List<FragmentMetadata> createFragment(
            String datasetUri, ArrowReader reader, WriteParams params) {
        try (ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrowStream);
            return Fragment.create(datasetUri, arrowStream, params);
        }
    }

    public static long appendFragments(LanceConfig config, List<FragmentMetadata> fragments) {
        FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
            Dataset datasetWrite =
                    Dataset.commit(
                            allocator,
                            config.getDatasetUri(),
                            appendOp,
                            java.util.Optional.of(datasetRead.version()),
                            options.getStorageOptions());
            long version = datasetWrite.version();
            datasetWrite.close();
            // Dataset.create returns version 1
            return version - 1;
        }
    }

    public static long getVersion(LanceConfig config) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
            // Dataset.create returns version 1
            return datasetRead.latestVersion() - 1;
        }
    }

    public static ArrowReader getArrowReader(
            LanceConfig config, List<String> columns, List<ColumnOrdering> columnOrderings) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset datasetRead = Dataset.open(allocator, uri, options)) {
            ScanOptions.Builder scanOptionBuilder = new ScanOptions.Builder();
            if (!columns.isEmpty()) {
                scanOptionBuilder.columns(columns);
            }
            if (!columnOrderings.isEmpty()) {
                scanOptionBuilder.setColumnOrderings(columnOrderings);
            }
            return datasetRead.newScan(scanOptionBuilder.build()).scanBatches();
        }
    }

    public static Optional<Long> getDatasetRowCount(LanceConfig config) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset dataset = Dataset.open(allocator, uri, options)) {
            return Optional.of(dataset.countRows());
        } catch (IllegalArgumentException e) {
            // dataset not found
            return Optional.empty();
        }
    }

    public static Schema getSchema(LanceConfig config) {
        String uri = config.getDatasetUri();
        ReadOptions options = LanceConfig.genReadOptionFromConfig(config);
        try (Dataset dataset = Dataset.open(allocator, uri, options)) {
            return dataset.getSchema();
        }
    }

    public static LanceArrowWriter getArrowWriter(
            Schema schema, int batchSize, TableBucket tableBucket) {
        return new LanceArrowWriter(allocator, schema, batchSize, tableBucket);
    }
}
