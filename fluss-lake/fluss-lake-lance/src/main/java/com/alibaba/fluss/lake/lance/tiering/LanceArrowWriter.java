package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A custom arrow reader that supports writes Fluss internal rows while reading data in batches. */
public class LanceArrowWriter extends ArrowReader {
    private final Schema schema;
    private final int batchSize;

    private volatile boolean finished;

    private final AtomicLong totalBytesRead = new AtomicLong();
    private ArrowWriter arrowWriter = null;
    private final AtomicInteger count = new AtomicInteger(0);
    private final Semaphore writeToken;
    private final Semaphore loadToken;
    private final int bucket;

    public LanceArrowWriter(
            BufferAllocator allocator, Schema schema, int batchSize, TableBucket tableBucket) {
        super(allocator);
        checkNotNull(schema);
        checkArgument(batchSize > 0);
        this.schema = schema;
        this.batchSize = batchSize;
        this.bucket = tableBucket.getBucket();
        this.writeToken = new Semaphore(0);
        this.loadToken = new Semaphore(0);
    }

    void write(LogRecord row) {
        checkNotNull(row);
        try {
            // wait util prepareLoadNextBatch to release write token,
            writeToken.acquire();
            arrowWriter.writeRow(row.getRow(), bucket, row.logOffset());
            if (count.incrementAndGet() == batchSize) {
                // notify loadNextBatch to take the batch
                loadToken.release();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void setFinished() {
        loadToken.release();
        finished = true;
    }

    @Override
    public void prepareLoadNextBatch() throws IOException {
        super.prepareLoadNextBatch();
        arrowWriter = ArrowWriter.create(this.getVectorSchemaRoot());
        // release batch size token for write
        writeToken.release(batchSize);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        try {
            if (finished && count.get() == 0) {
                return false;
            }
            // wait util batch if full or finished
            loadToken.acquire();
            arrowWriter.finish();
            if (!finished) {
                count.set(0);
                return true;
            } else {
                // true if it has some rows and return false if there is no record
                if (count.get() > 0) {
                    count.set(0);
                    return true;
                } else {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long bytesRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected synchronized void closeReadSource() throws IOException {
        // Implement if needed
    }

    @Override
    protected Schema readSchema() {
        return this.schema;
    }
}
