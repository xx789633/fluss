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

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;

/** The {@link SimpleVersionedSerializer} for {@link PaimonBucketOffset}. */
public class PaimonBucketOffsetSerializer implements SimpleVersionedSerializer<PaimonBucketOffset> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonBucketOffset bucketOffset) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);
        // write partition id
        if (bucketOffset.getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(bucketOffset.getPartitionId());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(bucketOffset.getBucket());

        // serialize partition name
        if (bucketOffset.getPartitionName() != null) {
            out.writeBoolean(true);
            out.writeUTF(bucketOffset.getPartitionName());
        } else {
            out.writeBoolean(false);
        }

        // serialize bucket offset
        out.writeLong(bucketOffset.getLogOffset());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public PaimonBucketOffset deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        Long partitionId = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
        }
        int bucket = in.readInt();

        // deserialize partition name
        String partitionName = null;
        if (in.readBoolean()) {
            partitionName = in.readUTF();
        }

        // deserialize bucket offset
        long bucketOffset = in.readLong();

        return new PaimonBucketOffset(bucketOffset, bucket, partitionId, partitionName);
    }
}
