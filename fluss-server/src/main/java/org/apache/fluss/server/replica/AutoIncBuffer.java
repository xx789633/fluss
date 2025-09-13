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

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.types.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.ArrayList;
import java.util.List;

public class AutoIncBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(AutoIncBuffer.class);

    private final String fieldName;
    private final ZkSequenceIDCounter autoIncrementColumnCounter;
    private final TablePath tablePath;
    private final int schemaId;
    private final ZooKeeperClient zkClient;
    private final int columnIdx;
    List<AutoIncRange> buffers;
    private long currentVolume = 0;
    private boolean isFetching = false;
    private long batchSize;
    private long autoIncLowWaterLevelMarkSizeRatio;
    private long autoIncPrefetchSizeRatio;

    public AutoIncBuffer(TablePath tablePath, int schemaId, int columnIdx, String fieldName, ZooKeeperClient zkClient) {
        this.fieldName = fieldName;
        this.tablePath = tablePath;
        this.schemaId = schemaId;
        this.columnIdx = columnIdx;
        this.zkClient = zkClient;
        this.autoIncrementColumnCounter = new ZkSequenceIDCounter(zkClient.getCuratorClient(), ZkData.AutoIncrementColumnZNode.path(tablePath, schemaId, columnIdx));
    }

    private List<Tuple2<Long, Long>> getAutoincRangesFromBuffers(MutableLong requestLength) {
        List<Tuple2<Long, Long>> result = new ArrayList<>();
        while (requestLength.getValue() > 0 && !buffers.isEmpty()) {
            AutoIncRange autoinc_range = buffers.get(0);
            long min_length = Math.min(requestLength.getValue(), autoinc_range.getLength());
            result.add(Tuple2.of(autoinc_range.getStart(), min_length));
            autoinc_range.consume(min_length);
            this.currentVolume -= min_length;
            requestLength.subtract(min_length);
            if (autoinc_range.empty()) {
                buffers.remove(0);
            }
        }
        return result;
    }

    public List<Tuple2<Long, Long>> syncRequestIds(long requestLength) {
        MutableLong mutableRequestLength = new MutableLong(requestLength);
        List<Tuple2<Long, Long>> result = new ArrayList<>();
        while (mutableRequestLength.getValue() > 0) {
            result.addAll(getAutoincRangesFromBuffers(mutableRequestLength));
            if (mutableRequestLength.getValue() == 0) {
                break;
            }
            if (!this.isFetching) {
                RETURN_IF_ERROR(
                        _launch_async_fetch_task(std::max<size_t>(request_length, prefetchSize())));
            }
            _rpc_token->wait();
            if (!_rpc_status.ok()) {
                return _rpc_status;
            }
        }
        if (!isFetching && currentVolume < low_water_level_mark()) {
            RETURN_IF_ERROR(_launch_async_fetch_task(prefetchSize()));
        }
    }

    private long prefetchSize() {
        return this.batchSize * this.autoIncPrefetchSizeRatio;
    }

    private long low_water_level_mark() {
        return this.batchSize * this.autoIncLowWaterLevelMarkSizeRatio;
    }
    private static class AutoIncRange {
        private long start;
        private long length;

        public AutoIncRange(long start, long length) {
            this.start = start;
            this.length = length;
        }

        public long getLength() {
            return length;
        }

        public long getStart() {return start;}

        public void consume(long l) {
            start += l;
            length -= l;
        }

        public boolean empty() { return length == 0;
        }
    }
}
