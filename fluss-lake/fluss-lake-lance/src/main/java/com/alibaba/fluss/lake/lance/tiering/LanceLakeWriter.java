package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/** Implementation of {@link LakeWriter} for Lance. */
public class LanceLakeWriter implements LakeWriter<LanceWriteResult> {
    private final LanceArrowWriter arrowWriter;
    FutureTask<List<FragmentMetadata>> fragmentCreationTask;

    public LanceLakeWriter(Configuration options, WriterInitContext writerInitContext) {
        TableBucket bucket = writerInitContext.tableBucket();
        LanceConfig config =
                LanceConfig.from(
                        options.toMap(),
                        writerInitContext.tablePath().getDatabaseName(),
                        writerInitContext.tablePath().getTableName());
        int batchSize = LanceConfig.getBatchSize(config);
        this.arrowWriter =
                LanceDatasetAdapter.getArrowWriter(
                        LanceDatasetAdapter.getSchema(config),
                        batchSize,
                        writerInitContext.tableBucket());

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        Callable<List<FragmentMetadata>> fragmentCreator =
                () ->
                        LanceDatasetAdapter.createFragment(
                                config.getDatasetUri(), arrowWriter, params);
        fragmentCreationTask = new FutureTask<>(fragmentCreator);
        Thread fragmentCreationThread = new Thread(fragmentCreationTask);
        fragmentCreationThread.start();
    }

    @Override
    public void write(LogRecord record) throws IOException {
        arrowWriter.write(record);
    }

    @Override
    public LanceWriteResult complete() throws IOException {
        arrowWriter.setFinished();
        try {
            List<FragmentMetadata> fragmentMetadata = fragmentCreationTask.get();
            return new LanceWriteResult(fragmentMetadata);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for reader thread to finish", e);
        } catch (ExecutionException e) {
            throw new IOException("Exception in reader thread", e);
        }
    }

    @Override
    public void close() throws IOException {
        arrowWriter.close();
    }
}
