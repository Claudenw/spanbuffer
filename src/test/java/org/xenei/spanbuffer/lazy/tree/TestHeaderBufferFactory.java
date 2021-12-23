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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;

public class TestHeaderBufferFactory implements BufferFactory {

    private int bufferSize;

    public TestHeaderBufferFactory(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public int bufferSize() {
        return bufferSize + 5;
    }

    @Override
    public ByteBuffer createBuffer() {
        /* create a buffer with a header that has a known fill */
        ByteBuffer bb = ByteBuffer.allocate(bufferSize());
        for (int i = 0; i < headerSize(); i++) {
            bb.put((byte) i);
        }
        return bb;
    }

    @Override
    public int headerSize() {
        return 5;
    }

    @Override
    public void free(ByteBuffer buffer) {
        /* verify the header is intact */
        for (int i = 0; i < headerSize(); i++) {
            assertEquals(String.format("on free() buffer header corrupted at %s", i), (byte) i, buffer.get(i));
        }
    }
}
