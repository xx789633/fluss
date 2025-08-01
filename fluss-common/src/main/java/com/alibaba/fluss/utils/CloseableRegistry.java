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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.shaded.guava32.com.google.common.collect.Lists.reverse;

/**
 * This class allows to register instances of {@link Closeable}, which are all closed if this
 * registry is closed.
 *
 * <p>Registering to an already closed registry will throw an exception and close the provided
 * {@link Closeable}
 *
 * <p>All methods in this class are thread-safe.
 *
 * <p>This class closes all registered {@link Closeable}s in the reverse registration order.
 */
@Internal
public class CloseableRegistry
        extends AbstractAutoCloseableRegistry<Closeable, Closeable, Object, IOException>
        implements Closeable {

    private static final Object DUMMY = new Object();

    public CloseableRegistry() {
        super(new LinkedHashMap<>());
    }

    @Override
    protected void doRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        closeableMap.put(closeable, DUMMY);
    }

    @Override
    protected boolean doUnRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        return closeableMap.remove(closeable) != null;
    }

    /**
     * This implementation doesn't imply any exception during closing due to backward compatibility.
     */
    @Override
    public void doClose(List<Closeable> toClose) throws IOException {
        IOUtils.closeAllQuietly(reverse(toClose));
    }

    /**
     * Unregisters all given {@link Closeable} objects from this registry and closes all objects
     * that are were actually registered. Suppressed (and collects) all exceptions that happen
     * during closing and throws only when the all {@link Closeable} objects have been processed.
     *
     * @param toUnregisterAndClose closables to unregister and close.
     * @throws IOException collects all exceptions encountered during closing of the given objects.
     */
    public void unregisterAndCloseAll(Closeable... toUnregisterAndClose) throws IOException {
        IOException suppressed = null;
        for (Closeable closeable : toUnregisterAndClose) {
            if (unregisterCloseable(closeable)) {
                try {
                    closeable.close();
                } catch (IOException ex) {
                    suppressed = ExceptionUtils.firstOrSuppressed(ex, suppressed);
                }
            }
        }

        if (suppressed != null) {
            throw suppressed;
        }
    }
}
