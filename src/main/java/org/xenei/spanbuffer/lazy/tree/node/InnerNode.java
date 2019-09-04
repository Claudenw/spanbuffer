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
import java.nio.ByteBuffer;

/**
 * An inner node in the tree buffer structure. This node contains serializer
 * encoded pointers to other nodes.
 */
public class InnerNode extends TreeNode {

	// the total length of this node as calculated by expanding the data in the leaf
	// nodes.
	private long length;
	/**
	 * The size in bytes of the inner node buffer flag.
	 */
	public static final int FLAG_SIZE = 1;
	/**
	 * The position in an inner node buffer of the flag
	 */
	public static final int FLAG_BYTE = 0;
	/**
	 * A flag that indicates the inner node contains buffer data after the flag
	 * byte.
	 */
	public static final byte OUTER_NODE_FLAG = 0x2;
	/**
	 * A flag that indicates the inner node contains pointers to leaf nodes.
	 */
	public static final byte LEAF_NODE_FLAG = 0x0;
	/**
	 * A flag that indicates the inner node contains pointers to other inner nodes.
	 */
	public static final byte INNER_NODE_FLAG = 0x1;

	/**
	 * Constructor.
	 *
	 * @param maxBufferSize max buffer size
	 * @param flag          represents the Node type we are dealing with. This
	 *                      indicated what types of nodes we point to.
	 * @throws IOException
	 */
	public InnerNode(BufferFactory factory, final byte flag) throws IOException {
		super(factory);
		data.put(typePosition(), flag);
		data.position(typePosition() + 1);
		length = 0;
	}

	/**
	 * Create an Outer Node type of inner node from the leaf node.
	 * 
	 * There must be space in the leaf node for the flag byte.
	 * 
	 * @param ln LeafNode with Data
	 * @throws IllegalStateException if the LeafNode data is too big for the Inner
	 *                               node
	 * @throws IOException
	 */
	public InnerNode(BufferFactory factory, final LeafNode ln) throws IllegalStateException, IOException {

		this(factory, OUTER_NODE_FLAG);

		/*
		 * check if the data would fit in the Inner node. The leaf node offset will be
		 * the number of bytes actually written to the buffer.
		 */
		if (hasSpace(ln.getUsedSpace())) {
			data.position(typePosition() + 1).put(ln.getData().position(span.getOffset()));
			length += ln.getExpandedLength();
			factory.free(ln.getRawBuffer());
		} else {
			throw new IllegalStateException("Leaf node data is too big for Inner the inner node.");
		}
	}

	private int typePosition() {
		return span.getOffset() + InnerNode.FLAG_BYTE;
	}

	@Override
	public void clearData() throws IOException {
		ByteBuffer newData = factory.createBuffer();
		int typePos = typePosition();
		newData.put(typePos, data.get(typePos));
		data = newData;
		// make sure we do not step on the flag data byte.
		// Arrays.fill(data, 1, data.length - 1, (byte) 0);
		data.position(typePos + 1);
		length = 0;
	}

	@Override
	public long getExpandedLength() {
		return length;
	}

	@Override
	public void adjustLength(final long expandedLength) {
		length += expandedLength;
	}

	/**
	 * Is the data stream empty with no data?.
	 *
	 * @return true = the stream is empty apart from the internal flag
	 */
	@Override
	public boolean isDataEmpty() {
		return data.position() == span.getOffset() + 1;
	}

	@Override
	public String toString() {
		return String.format("InnerNode[ t:%s ex:%s l:%s ]", data.get(typePosition()), length,
				data.position() - span.getOffset());
	}

}
