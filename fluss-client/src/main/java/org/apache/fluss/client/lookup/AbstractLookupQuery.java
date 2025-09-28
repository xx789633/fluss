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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import java.util.concurrent.CompletableFuture;

/** Abstract Class to represent a lookup operation. */
@Internal
public abstract class AbstractLookupQuery<T> {

    private final TableBucket tableBucket;
    private final byte[] key;
    private final boolean insertIfNotExists;

    public AbstractLookupQuery(TableBucket tableBucket, byte[] key, boolean insertIfNotExists) {
        this.tableBucket = tableBucket;
        this.key = key;
        this.insertIfNotExists = insertIfNotExists;
    }

    public byte[] key() {
        return key;
    }
    public boolean insertIfNotExists() {return insertIfNotExists;}

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public abstract LookupType lookupType();

    public abstract CompletableFuture<T> future();
}
