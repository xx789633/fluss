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

public class AutoIncrementColumnManager {
    private final String fieldName;
    private final ZkSequenceIDCounter autoIncrementColumnCounter;
    private final long interval = 10000;
    private final TablePath tablePath;
    private final int schemdId;
    private final ZooKeeperClient zkClient;
    private long cached_valued;

    public AutoIncrementColumnManager(TablePath tablePath, int schemaId, int columnIdx, String fieldName, ZooKeeperClient zkClient) {
        this.fieldName = fieldName;
        this.tablePath = tablePath;
        this.schemdId = schemaId;
        this.zkClient = zkClient;
        this.autoIncrementColumnCounter = new ZkSequenceIDCounter(zkClient.getCuratorClient(), ZkData.AutoIncrementColumnZNode.path(tablePath, schemaId, columnIdx));
    }

    public long nextVal() throws Exception {
        if (cached_valued >= interval) {
         cached_valued =
                 autoIncrementColumnCounter.getAndAdd(this.interval);
        }
        return cached_valued++;
    }
}
