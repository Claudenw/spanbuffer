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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.xenei.spanbuffer.lazy.tree.node.TreeNode;
import org.xenei.spanbuffer.lazy.tree.serde.Position;
import org.xenei.spanbuffer.lazy.tree.serde.AbstractSerde;
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.span.IntSpan;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;
import org.xenei.spanbuffer.lazy.tree.node.LeafNode;

/**
 * An output stream that writes data using a TreeSerializer implementation.
 *
 * Once the output stream is closed the position of the root node may be
 * retrieved. That position can then be used with the Corresponding
 * TreeDeserializer to create a TreeLazyLoader from which a Spanbuffer may be
 * obtained.
 *
 * @see TreeRoundTripTest example.
 *
 */
public class TreeOutputStream extends OutputStream {

	private boolean closed = false;
	@SuppressWarnings("rawtypes")
	protected final TreeSerializer serializer;
	protected Position position;
	protected final BufferFactory factory;

	private static final Logger LOG = LoggerFactory.getLogger(TreeOutputStream.class);

	/*
	 * the stack of nodes we are writing to. Leaf is node 0, the rest are inner
	 * nodes leading back to the root node.
	 */
	protected final List<TreeNode> stackNodeList;

	private static final int LEAF_NODE_INDEX = 0;
	private static final int FIRST_INNER_INDEX = 1;

	/**
	 * Constructor using a serde.
	 *
	 * @param serde
	 * @throws IOException
	 */
	public TreeOutputStream(AbstractSerde<?> serde) throws IOException {
		this(serde.getSerializer(), serde.getFactory());
	}

	/**
	 * Constructor. The factory must produce buffers that are 1 + (2*positionSize)
	 * long. Position size is specified by the serializer.
	 *
	 * @param serializer The TreeSerializer implementation to use for the output.
	 * @param factory    The Factory to produce new buffers.
	 * @throws IOException
	 */
	public TreeOutputStream(TreeSerializer<?> serializer, BufferFactory factory) throws IOException {
		this.factory = factory;
		this.serializer = serializer;
		this.position = serializer.getNoDataPosition();

		int minInnerLeafSize = 1 + (serializer.getPositionSize() * 2);
		if (factory.bufferSize() < minInnerLeafSize) {
			throw new IllegalArgumentException(
					String.format("Factory must produce buffers that are at least %s bytes long", minInnerLeafSize));
		}

		/* create the stack */
		stackNodeList = new ArrayList<>();
		stackNodeList.add(new LeafNode(factory));
		stackNodeList.add(new InnerNode(factory, InnerNode.LEAF_NODE_FLAG));
	}

	@Override
	public void write(final int arg0) throws IOException {

		if (closed) {
			throw new IOException("Stream is closed.");
		}
		writeLeafNode(ByteBuffer.wrap(new byte[] { (byte) arg0 }));
	}

	@Override
	public void write(final byte[] buffer, final int off, final int len) throws IOException {
		if (closed) {
			throw new IOException("Stream is closed.");
		}
		if (buffer == null) {
			throw new NullPointerException();
		} else if ((off < 0) || (off > buffer.length) || (len < 0) || ((off + len) > buffer.length)
				|| ((off + len) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return;
		}
		writeLeafNode(ByteBuffer.wrap(buffer, off, len));
	}

	/**
	 * Write the data buffer to the leaf nodes. The buffer may be larger than the
	 * leaf node so this code will split larger buffers across multiple leaf nodes
	 * and ensure that the proper inner nodes are constructed.
	 *
	 * @param data the data to write.
	 * @throws IOException on error.
	 */
	private void writeLeafNode(ByteBuffer data) throws IOException {

		/*
		 * The strategy here is to only create an inner node when there is actually data
		 * to write to it. Thus when we get to the close() processing it is much simpler
		 * and there are fewer allocated nodes that we may have to free.
		 */
		final TreeNode leafNode = stackNodeList.get(LEAF_NODE_INDEX);
		while (data.hasRemaining()) {
			// if the leaf node is full write it.
			if (!leafNode.hasSpace(1)) {
				ByteBuffer toWrite = serializeNode(leafNode);
				writeNode(toWrite, FIRST_INNER_INDEX, leafNode.getSpan().getLength());
				leafNode.clearData();
			}

			/*
			 * write some or all of the data depending on how much is left.
			 */
			int partLimit = Integer.min(data.remaining(), leafNode.getSpace());
			ByteBuffer part = data.duplicate();
			part.limit(data.position() + partLimit);
			writeNode(part, LEAF_NODE_INDEX, partLimit);
			data.position(part.limit());
		}

	}

	/**
	 * Writes to node. The data in the buffer must fit in an empty node, keep in
	 * mind that inner nodes are 1 byte shorter than a leaf node.
	 *
	 * @param buffer         data buffer to be written in the node
	 * @param nodeIndex      index position in the stackNodeList
	 * @param expandedLength the length of the actual data
	 * @throws IOException
	 *
	 */
	private void writeNode(ByteBuffer buffer, final int nodeIndex, final long expandedLength) throws IOException {

		// Node number should never be greater than the size
		if (nodeIndex >= stackNodeList.size()) {
			stackNodeList.add(new InnerNode(factory, InnerNode.INNER_NODE_FLAG));
		}

		final TreeNode node = stackNodeList.get(nodeIndex);

		if (!node.hasSpace(buffer.remaining())) {
			// the current node does not have room for the remaining data
			// write what we can.
			LOG.debug( "Can not write {} into buffer space of {}", buffer.remaining(), node.getSpace());
			final ByteBuffer bb = serializeNode(node);
			writeNode(bb, nodeIndex + 1, node.getExpandedLength());
			node.clearData();
		}
		node.write(buffer, expandedLength);

	}

	/**
	 * Create the root node. This method is called after close and ensures that all
	 * the data are preserved.
	 *
	 * @throws InterruptedException In case action was interrupted during execution
	 * @throws ExecutionException   Thrown at execution
	 * @throws IOException
	 */
	private void createRoot() throws IOException {
		int nodeIndex = LEAF_NODE_INDEX;

		/*
		 * Handle the special case where all of the data in the leaf node will fit in
		 * the inner node. This creates an "OUTER_NODE" type that means we only have one
		 * node to track.
		 */

		if (stackNodeList.size() == 2) {
			LeafNode leaf = (LeafNode) stackNodeList.get(LEAF_NODE_INDEX);
			TreeNode inner = stackNodeList.get(FIRST_INNER_INDEX);
			if (inner.isDataEmpty() && inner.hasSpace(leaf.getUsedSpace())) {
				LOG.debug("Creating OUTER Node");
				// create a reference to the root node
				// Let's create our root node, constructor frees leaf
				final TreeNode rootNode = new InnerNode(factory, leaf);
				// ensure that the leaf data is freed and cleared.
				factory.free(inner.getRawBuffer());
				writeRoot(rootNode);
				return;
			}
		}

		TreeNode node = stackNodeList.get(nodeIndex);

		while (nodeIndex < stackNodeList.size() - 1) {
			final int nextPosition = nodeIndex + 1;
			final ByteBuffer bb = serializeNode(node);
			writeNode(bb, nextPosition, node.getExpandedLength());
			nodeIndex = nextPosition;
			node = stackNodeList.get(nodeIndex);
		}
		// We are at the last node of the stack
		writeRoot(node);
	}

	/**
	 * Once we are closed we write root node. Its position is retrieved with this
	 * method.
	 *
	 * @return Position object for the root
	 * @throws IllegalStateException if the output is not closed.
	 */
	@SuppressWarnings("unchecked")
	public <T extends Position> T getPosition() {

		if (closed) {
			return (T) position;
		} else {
			throw new IllegalStateException("TreeOutputStream is not closed");
		}
	}

	/**
	 * Writes the node's data and retrieves the position in a buffer for an inner
	 * node.
	 *
	 * @param node The tree node to serialize.
	 * @return the position encoded into a bytebuffer by the TreeSerializer.
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private ByteBuffer serializeNode(final TreeNode node) throws IOException {

		TreeOutputStream.LOG.debug("Writing {} ", node);
		return serializer.serialize(serializer.serialize(node.getRawBuffer()));

	}

	/**
	 * Writes the data for the root node using the serializer. Updates the final
	 * position.
	 *
	 * @param rootNode The TreeNode that is the root node.
	 * @return Position information from TreeSerializer.
	 * @throws ExecutionException   on error
	 * @throws InterruptedException on error
	 * @throws IOException
	 */
	protected void writeRoot(final TreeNode rootNode) throws IOException {

		TreeOutputStream.LOG.debug("Writing Root Node: {}", rootNode);
		position = serializer.serialize(rootNode.getRawBuffer());
	}

	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		super.close();
		closed = true;
		if (stackNodeList.get(LEAF_NODE_INDEX).isDataEmpty()) {
			factory.free(stackNodeList.get(LEAF_NODE_INDEX).getData());
			position = serializer.getNoDataPosition();
		} else {
			createRoot();
		}
	}
}
