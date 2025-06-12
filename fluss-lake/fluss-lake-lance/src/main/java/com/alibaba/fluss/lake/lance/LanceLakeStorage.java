package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lance.tiering.LanceCommittable;
import com.alibaba.fluss.lake.lance.tiering.LanceLakeTieringFactory;
import com.alibaba.fluss.lake.lance.tiering.LanceWriteResult;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;

/** Lance implementation of {@link LakeStorage}. */
public class LanceLakeStorage implements LakeStorage {
    private final Configuration config;

    public LanceLakeStorage(Configuration configuration) {
        this.config = configuration;
    }

    @Override
    public LakeTieringFactory<LanceWriteResult, LanceCommittable> createLakeTieringFactory() {
        return new LanceLakeTieringFactory(config);
    }

    @Override
    public LanceLakeCatalog createLakeCatalog() {
        return new LanceLakeCatalog(config);
    }
}
