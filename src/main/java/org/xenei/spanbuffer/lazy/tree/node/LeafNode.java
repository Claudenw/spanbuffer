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
package org.xenei.spanbuffer.lazy.tree.node;

import java.util.Arrays;

/**
 * A leaf node in a tree buffer structure.  This node contains raw byte data for the span buffer.
 */
public class LeafNode extends TreeNode {

    /**
     * Constructor.
     *
     * @param maxBufferSize
     *            max buffer size
     */
    public LeafNode(final int maxBufferSize) {
        super( maxBufferSize );
    }

    @Override
    public boolean isDataEmpty() {
        return offset == 0;
    }

    @Override
    protected void adjustLength(final long expandedLength) throws IllegalStateException {
        // Do nothing
    }

    @Override
    public long getExpandedLength() {
        return offset;
    }

    @Override
    public void clearData() {
        Arrays.fill( data, (byte) 0 );
        offset = 0;
    }

    @Override
    public String toString() {
        return "LeafNode of length: " + getExpandedLength();
    }

}