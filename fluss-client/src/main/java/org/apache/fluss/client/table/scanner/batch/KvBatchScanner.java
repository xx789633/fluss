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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PBNewScanReq;
import org.apache.fluss.rpc.messages.PBScanReq;
import org.apache.fluss.rpc.messages.PBScanResp;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A {@link BatchScanner} implementation that scans a limited number of records from a table. */
public class KvBatchScanner implements BatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);

    private final TableInfo tableInfo;
    @Nullable private final int[] projectedFields;
    private final int limit;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final TableBucket tableBucket;
    private final ValueDecoder kvValueDecoder;

    private boolean endOfInput;

    /** Maximum number of bytes returned by the scanner, on each batch. */
    private final int batchSizeBytes = 10000;

    private boolean closed = false;
    private boolean opened = false;
    private boolean canRequestMore = true;
    private long numRowsReturned = 0;

    /**
     * This is the scanner ID we got from the TabletServer. It's generated randomly so any value is
     * possible.
     */
    private byte[] scannerId;

    /**
     * The sequence ID of this call. The sequence ID should start at 0 with the request for a new
     * scanner, and after each successful request, the client should increment it by 1. When
     * retrying a request, the client should _not_ increment this value. If the server detects that
     * the client missed a chunk of rows from the middle of a scan, it will respond with an error.
     */
    private int sequenceId;

    private TabletServerGateway gateway;
    private CompletableFuture<PBScanResp> scanFuture;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            int limit) {
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.tableBucket = tableBucket;

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }

        if (tableBucket.getPartitionId() != null) {
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        }

        int leader = metadataUpdater.leaderFor(tableBucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            // TODO handle this exception, like retry.
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }

        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
        this.endOfInput = false;
    }

    private enum State {
        OPENING,
        NEXT,
        CLOSING
    }

    /** Returns an RPC to open this scanner. */
    PBScanReq getOpenRequest() {
        checkScanningNotStarted();
        return createRequestPB(tableInfo, State.OPENING, tableBucket);
    }

    PBScanReq createRequestPB(TableInfo tableInfo, State state, TableBucket tableBucket) {
        PBScanReq builder = new PBScanReq();
        switch (state) {
            case OPENING:
                PBNewScanReq newBuilder = new PBNewScanReq();
                newBuilder.setTableId(tableInfo.getTableId());
                newBuilder.setBucketId(tableBucket.getBucket());
                if (tableBucket.getPartitionId() != null) {
                    newBuilder.setPartitionId(tableBucket.getPartitionId());
                }
                newBuilder.setLimit(limit);
                builder.setNewScanRequest(newBuilder).setBatchSizeBytes(this.batchSizeBytes);
                break;
            case NEXT:
                builder.setScannerId(scannerId)
                        .setCallSeqId(this.sequenceId)
                        .setBatchSizeBytes(batchSizeBytes);
                break;
            case CLOSING:
                builder.setScannerId(scannerId).setBatchSizeBytes(0).setCloseScanner(true);
                break;
            default:
                throw new RuntimeException("unreachable!");
        }

        return builder;
    }

    /** Returns an RPC to fetch the next rows. */
    PBScanReq getNextRowsRequest() {
        return createRequestPB(tableInfo, State.OPENING, tableBucket);
    }

    /**
     * Throws an exception if scanning already started.
     *
     * @throws IllegalStateException if scanning already started.
     */
    private void checkScanningNotStarted() {
        if (opened) {
            throw new IllegalStateException("scanning already started");
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        try {
            if (closed) { // We're already done scanning.
                throw new IllegalStateException("Scanner has already been closed");
            } else if (!opened) {
                PBScanReq scanReq = getOpenRequest();

                // We need to open the scanner first.
                scanFuture =
                        gateway.kvScan(scanReq)
                                .whenComplete(
                                        (r, t) -> {
                                            if (t != null) {
                                                if (!r.isHasMoreResults()
                                                        || r.getScannerId() == null) {
                                                    scanFinished();
                                                }
                                                scannerId = r.getScannerId();
                                                sequenceId++;
                                                canRequestMore = r.isHasMoreResults();
                                                opened = true;
                                            }
                                        });
                PBScanResp response = scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                List<InternalRow> scanRows =
                        parseKvScanResponse(response); // there might be data to return
                return CloseableIterator.wrap(scanRows.iterator());
            }
            scanFuture =
                    gateway.kvScan(getNextRowsRequest())
                            .whenComplete(
                                    (r, t) -> {
                                        if (t != null) {
                                            if (!r.isHasMoreResults()) { // We're done scanning this
                                                // tablet.
                                                scanFinished();
                                            }
                                            sequenceId++;
                                            canRequestMore = r.isHasMoreResults();
                                        }
                                    });
            PBScanResp response = scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            List<InternalRow> scanRows = parseKvScanResponse(response);
            return CloseableIterator.wrap(scanRows.iterator());
        } catch (TimeoutException e) {
            // poll next time
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    void scanFinished() {
        if (numRowsReturned >= limit) {
            canRequestMore = false;
            closed = true; // the scanner is closed on the other side at this point
            return;
        }
        scannerId = null;
        sequenceId = 0;
    }

    private List<InternalRow> parseKvScanResponse(PBScanResp scanResponse) {
        if (!scanResponse.hasRecords()) {
            return Collections.emptyList();
        }
        List<InternalRow> scanRows = new ArrayList<>();
        ByteBuffer recordsBuffer = ByteBuffer.wrap(scanResponse.getRecords());
        if (tableInfo.hasPrimaryKey()) {
            DefaultValueRecordBatch valueRecords =
                    DefaultValueRecordBatch.pointToByteBuffer(recordsBuffer);
            ValueRecordReadContext readContext =
                    new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
            for (ValueRecord record : valueRecords.records(readContext)) {
                scanRows.add(maybeProject(record.getRow()));
            }
        } else {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(tableInfo, false, null);
            LogRecords records = MemoryLogRecords.pointToByteBuffer(recordsBuffer);
            for (LogRecordBatch logRecordBatch : records.batches()) {
                // A batch of log record maybe little more than limit, thus we need slice the
                // last limit number.
                try (CloseableIterator<LogRecord> logRecordIterator =
                        logRecordBatch.records(readContext)) {
                    while (logRecordIterator.hasNext()) {
                        scanRows.add(maybeProject(logRecordIterator.next().getRow()));
                    }
                }
            }
        }
        if (scanRows.size() > limit) {
            scanRows = scanRows.subList(scanRows.size() - limit, scanRows.size());
        }
        return scanRows;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // TODO: currently, we have to deep copy the row to avoid the underlying ArrowBatch is
        //  released, we should return the originRow directly and lazily deserialize ArrowBatch in
        //  the future
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
            projectedRow.replaceRow(newRow);
            return projectedRow;
        } else {
            return newRow;
        }
    }

    @Override
    public void close() throws IOException {
        PBScanReq closeReq = createRequestPB(tableInfo, State.CLOSING, tableBucket);
        gateway.kvScan(closeReq);
        closed = true;
        scanFuture.cancel(true);
    }
}
