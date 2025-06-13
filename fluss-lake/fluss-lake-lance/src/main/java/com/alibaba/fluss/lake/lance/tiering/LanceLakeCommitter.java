package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.TablePath;

import com.lancedb.lance.FragmentMetadata;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;

/** Implementation of {@link LakeCommitter} for Lance. */
public class LanceLakeCommitter implements LakeCommitter<LanceWriteResult, LanceCommittable> {
    private final LanceConfig config;

    public LanceLakeCommitter(Configuration options, TablePath tablePath) {
        this.config =
                LanceConfig.from(
                        options.toMap(), tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public LanceCommittable toCommitable(List<LanceWriteResult> lanceWriteResults)
            throws IOException {
        List<FragmentMetadata> fragments =
                lanceWriteResults.stream()
                        .map(LanceWriteResult::commitMessage)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return new LanceCommittable(fragments);
    }

    @Override
    public long commit(LanceCommittable committable) throws IOException {
        return LanceDatasetAdapter.appendFragments(config, committable.committable());
    }

    @Override
    public void abort(LanceCommittable committable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("checkstyle:LocalVariableName")
    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        long latestLakeSnapshotIdOfLake = LanceDatasetAdapter.getVersion(config);

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotIdOfLake <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshotIdOfLake);

        long maxOffset = 0;
        ArrowReader reader = LanceDatasetAdapter.getColumnReader(config, OFFSET_COLUMN_NAME);
        while (reader.loadNextBatch()) {
            BigIntVector iVector = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
            for (int i = 0; i < iVector.getValueCount(); i++) {
                maxOffset = Math.max(iVector.get(i), maxOffset);
            }
        }
        committedLakeSnapshot.addBucket(0, maxOffset);
        return committedLakeSnapshot;
    }

    @Override
    public void close() throws Exception {}
}
