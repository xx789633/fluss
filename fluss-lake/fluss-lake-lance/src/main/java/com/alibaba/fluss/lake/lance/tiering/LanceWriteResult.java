package com.alibaba.fluss.lake.lance.tiering;

import com.lancedb.lance.FragmentMetadata;

import java.util.List;

/** The write result of Lance lake writer to pass to commiter to commit. */
public class LanceWriteResult {
    private static final long serialVersionUID = 1L;

    private final List<FragmentMetadata> commitMessage;

    public LanceWriteResult(List<FragmentMetadata> commitMessage) {
        this.commitMessage = commitMessage;
    }

    public List<FragmentMetadata> commitMessage() {
        return commitMessage;
    }
}
