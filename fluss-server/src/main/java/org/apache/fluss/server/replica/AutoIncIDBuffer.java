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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.concurrent.Scheduler;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The buffer for auto increment column IDs. */
public class AutoIncIDBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(AutoIncIDBuffer.class);

    private final ZkSequenceIDCounter autoIncZkCounter;
    private final TablePath tablePath;
    private final int schemaId;
    private final int columnIdx;
    private final String columnName;

    private final Object lock = new Object();

    @GuardedBy("lock")
    List<AutoIncRange> buffers;

    @GuardedBy("lock")
    private long currentVolume = 0;

    private boolean isFetching = false;
    private final long prefetchBatchSize;
    private final double lowWaterMarkRatio;
    private ScheduledFuture<?> prefetchTask;
    private final Scheduler fetchAutoincIdScheduler;

    public AutoIncIDBuffer(
            TablePath tablePath,
            int schemaId,
            int columnIdx,
            String columnName,
            ZooKeeperClient zkClient,
            Scheduler scheduler,
            Configuration properties) {
        lowWaterMarkRatio =
                properties.getDouble(ConfigOptions.TABLE_AUTO_INC_PREFETCH_LOW_WATER_MARK_RATIO);
        prefetchBatchSize = properties.getLong(ConfigOptions.TABLE_AUTO_INC_PREFETCH_BATCH_SIZE);
        checkArgument(
                lowWaterLevelMark() > 0, "auto inc low level water mark must be greater than 0");
        this.columnName = columnName;
        this.tablePath = tablePath;
        this.schemaId = schemaId;
        this.columnIdx = columnIdx;
        this.fetchAutoincIdScheduler = scheduler;
        this.buffers = new ArrayList<>();
        this.autoIncZkCounter =
                new ZkSequenceIDCounter(
                        zkClient.getCuratorClient(),
                        ZkData.AutoIncrementColumnZNode.path(tablePath, schemaId, columnIdx));
    }

    private List<Tuple2<Long, Long>> getAutoIncRangesFromBuffers(MutableLong requestLength) {
        List<Tuple2<Long, Long>> result = new ArrayList<>();
        synchronized (lock) {
            while (requestLength.getValue() > 0 && !buffers.isEmpty()) {
                AutoIncRange autoincRange = buffers.get(0);
                checkArgument(autoincRange.getLength() > 0);
                long minLength = Math.min(requestLength.getValue(), autoincRange.getLength());
                result.add(Tuple2.of(autoincRange.getStart(), minLength));
                autoincRange.consume(minLength);
                currentVolume -= minLength;
                requestLength.subtract(minLength);
                if (autoincRange.empty()) {
                    buffers.remove(0);
                }
            }
        }
        return result;
    }

    public int getColumnIdx() {
        return columnIdx;
    }

    public List<Tuple2<Long, Long>> syncRequestIds(long requestLength)
            throws ExecutionException, InterruptedException {
        MutableLong length = new MutableLong(requestLength);
        List<Tuple2<Long, Long>> result = new ArrayList<>();
        while (length.getValue() > 0) {
            result.addAll(getAutoIncRangesFromBuffers(length));
            if (length.getValue() == 0) {
                break;
            }
            if (!isFetching) {
                prefetchTask =
                        fetchAutoincIdScheduler.scheduleOnce(
                                "fetch-ids-from-zk",
                                () ->
                                        launchAsyncFetchTask(
                                                Math.max(length.getValue(), prefetchBatchSize)));
            }
            prefetchTask.get();
            checkArgument(!isFetching);
        }
        checkArgument(length.getValue() == 0);
        if (!isFetching && currentVolume < lowWaterLevelMark()) {
            prefetchTask =
                    fetchAutoincIdScheduler.scheduleOnce(
                            "fetch-ids-from-zk", () -> launchAsyncFetchTask(prefetchBatchSize));
        }
        return result;
    }

    private void launchAsyncFetchTask(long length) {
        this.isFetching = true;
        try {
            long start = autoIncZkCounter.getAndAdd(length);
            LOG.info(
                    "[AutoIncIDBuffer::launchAsyncFetchTask] successfully fetch auto-increment values from zookeeper, table_path={}, schema_id={}, column_idx={}, column_name={}",
                    tablePath,
                    schemaId,
                    columnIdx,
                    columnName);
            synchronized (lock) {
                buffers.add(new AutoIncRange(start, length));
                currentVolume += length;
            }
            this.isFetching = false;
        } catch (Exception e) {
            this.isFetching = false;
            throw new RuntimeException(
                    String.format(
                            "[AutoIncIDBuffer::launchAsyncFetchTask] failed to fetch auto-increment values from zookeeper, table_path=%s, schema_id=%d, column_idx=%d, column_name=%s",
                            tablePath, schemaId, columnIdx, columnName),
                    e);
        }
    }

    public long nextVal() throws ExecutionException, InterruptedException {
        List<Tuple2<Long, Long>> res = syncRequestIds(1);
        return res.get(0).f0;
    }

    private long lowWaterLevelMark() {
        return (long) (this.prefetchBatchSize * this.lowWaterMarkRatio);
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

        public long getStart() {
            return start;
        }

        public void consume(long l) {
            start += l;
            length -= l;
        }

        public boolean empty() {
            return length == 0;
        }
    }
}
