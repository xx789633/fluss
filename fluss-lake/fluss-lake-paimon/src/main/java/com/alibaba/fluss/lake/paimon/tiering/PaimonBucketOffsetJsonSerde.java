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

import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link PaimonBucketOffset}. */
public class PaimonBucketOffsetJsonSerde
        implements JsonSerializer<PaimonBucketOffset>, JsonDeserializer<PaimonBucketOffset> {

    public static final PaimonBucketOffsetJsonSerde INSTANCE = new PaimonBucketOffsetJsonSerde();
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_ID = "bucket_id";
    private static final String PARTITION_NAME = "partition_name";
    private static final String LOG_OFFSET = "log_offset";

    @Override
    public PaimonBucketOffset deserialize(JsonNode node) {
        JsonNode partitionIdNode = node.get(PARTITION_ID);
        Long partitionId = partitionIdNode == null ? null : partitionIdNode.asLong();
        int bucketId = node.get(BUCKET_ID).asInt();

        // deserialize partition name
        JsonNode partitionNameNode = node.get(PARTITION_NAME);
        String partitionName = partitionNameNode == null ? null : partitionNameNode.asText();

        // deserialize log offset
        long logOffset = node.get(LOG_OFFSET).asLong();

        return new PaimonBucketOffset(logOffset, bucketId, partitionId, partitionName);
    }

    @Override
    public void serialize(PaimonBucketOffset bucketOffset, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // write partition id
        if (bucketOffset.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, bucketOffset.getPartitionId());
        }
        generator.writeNumberField(BUCKET_ID, bucketOffset.getBucket());

        // serialize partition name
        if (bucketOffset.getPartitionName() != null) {
            generator.writeStringField(PARTITION_NAME, bucketOffset.getPartitionName());
        }

        // serialize bucket offset
        generator.writeNumberField(LOG_OFFSET, bucketOffset.getLogOffset());

        generator.writeEndObject();
    }

    /** Serialize the {@link PaimonBucketOffset} to json bytes. */
    public static byte[] toJson(PaimonBucketOffset paimonBucketOffset) {
        return JsonSerdeUtils.writeValueAsBytes(paimonBucketOffset, INSTANCE);
    }

    /** Deserialize the json bytes to {@link PaimonBucketOffset}. */
    public static PaimonBucketOffset fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }
}
