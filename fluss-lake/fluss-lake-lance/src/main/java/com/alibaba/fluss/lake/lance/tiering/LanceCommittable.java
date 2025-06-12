package com.alibaba.fluss.lake.lance.tiering;

import com.lancedb.lance.FragmentMetadata;
import java.util.List;

/** The committable that derived from {@link LanceWriteResult} to commit to Lance. */
public class LanceCommittable {
    private final List<FragmentMetadata> committable;

    public LanceCommittable(List<FragmentMetadata> committable) {
        this.committable = committable;
    }

    public List<FragmentMetadata> committable() {
        return committable;
    }
}
