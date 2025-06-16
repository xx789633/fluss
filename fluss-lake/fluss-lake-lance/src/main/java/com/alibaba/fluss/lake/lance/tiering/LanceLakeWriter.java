package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import java.io.IOException;

/** Implementation of {@link LakeWriter} for Lance. */
public class LanceLakeWriter implements LakeWriter<LanceWriteResult> {
    private final LanceArrowWriter arrowWriter;

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
    }

    @Override
    public void write(LogRecord record) throws IOException {
        arrowWriter.write(record);
    }

    @Override
    public LanceWriteResult complete() throws IOException {
        arrowWriter.setFinished();
        return null;
    }

    @Override
    public void close() throws IOException {
        arrowWriter.close();
    }
}
