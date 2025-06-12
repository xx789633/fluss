package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;

/** Lance implementation of {@link LakeStoragePlugin}. */
public class LanceLakeStoragePlugin implements LakeStoragePlugin {

    private static final String IDENTIFIER = "lance";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new LanceLakeStorage(configuration);
    }
}
