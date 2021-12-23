/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.spanbuffer.lazy.tree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.AbstractLazyLoader;
import org.xenei.spanbuffer.lazy.tree.serde.Position;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;

/**
 * Class that manages the loading of tree data.
 *
 * @param <P> the Position implementation.
 *
 *            This class always returns buffers at offset 0.
 *
 */
public class TreeLazyLoader<P extends Position> extends AbstractLazyLoader {

    private final TreeDeserializer<P> deserializer;
    private final P position;

    /**
     * Constructor.
     *
     * @param position     The position in the deserializer to read the data from.
     * @param deserializer the deserializer to read from.
     */
    public TreeLazyLoader(final P position, final TreeDeserializer<P> deserializer) {
        this(null, position, deserializer);
    }

    /**
     * Constructor.
     * @param length       The length of the buffer.
     * @param position     The position in the deserializer to read the data from.
     * @param deserializer the deserializer to read from.
     */
    public TreeLazyLoader(final Long length, final P position, final TreeDeserializer<P> deserializer) {
        super(length);
        this.deserializer = deserializer;
        this.position = position;
    }

    @Override
    public SpanBuffer getBuffer(int inset) {
        if (position.isNoData()) {
            return Factory.EMPTY;
        }
        return new InnerBuffer<P>(0, this).cut(inset);
    }

    /**
     * Apply the deserializer map to the buffer to create a list of TreeLazyLoaders.
     *
     * @return a list of TreeLazyLoader<P,T> from the buffer.
     * @throws IOException on error
     */
    public final List<TreeLazyLoader<P>> applyMap(SpanBuffer buffer) throws IOException {
        return deserializer.extractLoaders(buffer);
    }

    /**
     * Get the position this lazy loader is using
     *
     * @return the position.
     */
    public final P getPosition() {
        return position;
    }

    /**
     * Return true if the buffer has no data.
     *
     * @return true if the buffer has no data.
     */
    public final boolean hasNoData() {
        return position.isNoData();
    }

    @Override
    protected SpanBuffer getBufferInternal(int inset) throws IOException {
        ByteBuffer bb = deserializer.deserialize(position);
        SpanBuffer buffer = Factory.wrap(bb);
        return (inset == 0) ? buffer : buffer.cut(inset);
    }

}
