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
