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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.IOException;

/** The {@link SimpleVersionedSerializer} for {@link PaimonWriteResult}. */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    private static final int CURRENT_VERSION = 1;

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriteResult paimonWriteResult) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);

        // serialize write result
        byte[] serializeBytes = messageSer.serialize(paimonWriteResult.commitMessage());
        out.writeInt(serializeBytes.length);
        out.write(serializeBytes);

        serializeBytes = PaimonBucketOffsetJsonSerde.toJson(paimonWriteResult.bucketOffset());
        out.writeInt(serializeBytes.length);
        out.write(serializeBytes);

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting PaimonWriteResult version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        byte[] writeResultBytes = new byte[in.readInt()];
        in.readFully(writeResultBytes);
        CommitMessage commitMessage =
                messageSer.deserialize(messageSer.getVersion(), writeResultBytes);

        byte[] bucketOffsetBytes = new byte[in.readInt()];
        in.readFully(bucketOffsetBytes);
        PaimonBucketOffset bucketOffset = PaimonBucketOffsetJsonSerde.fromJson(bucketOffsetBytes);

        return new PaimonWriteResult(commitMessage, bucketOffset);
    }
}
