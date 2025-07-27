/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import javax.annotation.Nullable;

import java.io.Serializable;

/** The bucket offset information to be stored in Paimon's snapshot property. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaimonBucketOffset implements Serializable {

    private static final long serialVersionUID = 1L;

    private long logOffset;
    private int bucket;
    private @Nullable Long partitionId;
    private @Nullable String partitionName;

    public PaimonBucketOffset() {}

    public PaimonBucketOffset(
            long logOffset,
            int bucket,
            @Nullable Long partitionId,
            @Nullable String partitionName) {
        this.logOffset = logOffset;
        this.bucket = bucket;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public int getBucket() {
        return bucket;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public void setLogOffset(long logOffset) {
        this.logOffset = logOffset;
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}
