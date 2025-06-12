package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.lancedb.lance.FragmentMetadata;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import java.util.stream.Collectors;

/** Implementation of {@link LakeCommitter} for Lance. */
public class LanceLakeCommitter implements LakeCommitter<LanceWriteResult, LanceCommittable> {
    @Override
    public LanceCommittable toCommitable(List<LanceWriteResult> lanceWriteResults) throws IOException {
        List<FragmentMetadata> fragments =
                lanceWriteResults.stream()
                        .map(LanceWriteResult::commitMessage)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return new LanceCommittable(fragments);
    }

    @Override
    public long commit(LanceCommittable committable) throws IOException {
        LanceDatasetAdapter.appendFragments(config, committable.committable());
    }

    @Override
    public void abort(LanceCommittable committable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss) throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
