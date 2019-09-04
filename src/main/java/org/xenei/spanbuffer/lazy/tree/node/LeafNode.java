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

import java.io.IOException;

/**
 * A leaf node in a tree buffer structure. This node contains raw byte data for
 * the span buffer.
 */
public class LeafNode extends TreeNode {

	/**
	 * Constructor.
	 *
	 * @param maxBufferSize max buffer size
	 * @throws IOException
	 */
	public LeafNode(BufferFactory factory) throws IOException {
		super(factory);
	}

	@Override
	public boolean isDataEmpty() {
		return data.position() == span.getOffset();
	}

	@Override
	protected void adjustLength(final long expandedLength) throws IllegalStateException {
		// Do nothing
	}

	@Override
	public long getExpandedLength() {
		return data.limit() - data.remaining() - span.getOffset();
	}

	@Override
	public void clearData() throws IOException {
		data = factory.createBuffer();
	}

	@Override
	public String toString() {
		return String.format("LeafNode[ l:%s(%s) c:%s ]", data.position(), getExpandedLength(), data.capacity());
	}

}