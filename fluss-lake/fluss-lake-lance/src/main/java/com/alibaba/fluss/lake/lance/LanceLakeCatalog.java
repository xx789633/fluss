package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import com.lancedb.lance.WriteParams;

/** A Lance implementation of {@link LakeCatalog}. */
public class LanceLakeCatalog implements LakeCatalog {
    private Configuration config;
    public LanceLakeCatalog(Configuration config) {
        this.config = config;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {

        LanceConfig config = LanceConfig.from(config);
        WriteParams params = config.genWriteParamsFromConfig();
        LanceDatasetAdapter.createDataset("", tableDescriptor, params);
    }

    @Override
    public void close() throws Exception {
        LakeCatalog.super.close();
    }
}
