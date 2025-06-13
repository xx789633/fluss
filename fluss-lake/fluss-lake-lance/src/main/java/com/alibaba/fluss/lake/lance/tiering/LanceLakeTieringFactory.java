package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.CommitterInitContext;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for Lance . */
public class LanceLakeTieringFactory
        implements LakeTieringFactory<LanceWriteResult, LanceCommittable> {
    private final Configuration config;

    public LanceLakeTieringFactory(Configuration config) {
        this.config = config;
    }

    @Override
    public LakeWriter<LanceWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LanceWriteResult> getWriteResultSerializer() {
        return new LanceWriteResultSerializer();
    }

    @Override
    public LakeCommitter<LanceWriteResult, LanceCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new LanceLakeCommitter(config, committerInitContext.tablePath());
    }

    @Override
    public SimpleVersionedSerializer<LanceCommittable> getCommitableSerializer() {
        return new LanceCommittableSerializer();
    }
}
