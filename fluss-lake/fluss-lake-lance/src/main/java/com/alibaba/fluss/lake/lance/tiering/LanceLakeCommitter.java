package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.TablePath;

import com.lancedb.lance.FragmentMetadata;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
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

        LinkedHashMap<Integer, Long> bucketEndOffset = new LinkedHashMap<>();
        ArrowReader reader =
                LanceDatasetAdapter.getArrowReader(
                        config,
                        Arrays.asList(BUCKET_COLUMN_NAME, OFFSET_COLUMN_NAME),
                        Arrays.asList());
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            IntVector bucketVector = (IntVector) readerRoot.getVector(BUCKET_COLUMN_NAME);
            BigIntVector offsetVector = (BigIntVector) readerRoot.getVector(OFFSET_COLUMN_NAME);
            for (int i = 0; i < bucketVector.getValueCount(); i++) {
                if (!bucketEndOffset.containsKey(bucketVector.get(i))
                        || bucketEndOffset.get(bucketVector.get(i)) < offsetVector.get(i)) {
                    bucketEndOffset.put(bucketVector.get(i), offsetVector.get(i));
                }
            }
        }
        for (Map.Entry<Integer, Long> entry : bucketEndOffset.entrySet()) {
            committedLakeSnapshot.addBucket(entry.getKey(), entry.getValue());
        }
        return committedLakeSnapshot;
    }

    @Override
    public void close() throws Exception {}
}
