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
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	private final BufferFactory factory;

	private static final Logger LOG = LoggerFactory.getLogger(TreeOutputStream.class);

	/*
	 * the stack of nodes we are writing to. Leaf is node 0, the rest are inner
	 * nodes leading back to the root node.
	 */
	private final List<TreeNode> stackNodeList;

	private static final int LEAF_NODE_INDEX = 0;
	private static final int FIRST_INNER_INDEX = 1;

	/**
	 * Constructor. The factory must produce buffers that are 1 + (2*positionSize)
	 * long. Position size is specified by the serializer.
	 * 
	 * @param serializer The TreeSerializer implementation to use for the output.
	 * @param factory    The Factory to produce new buffers.
	 * @throws IOException
	 */
	public TreeOutputStream(@SuppressWarnings("rawtypes") TreeSerializer serializer, BufferFactory factory)
			throws IOException {
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

	private void writeLeafNode(ByteBuffer data) throws IOException {

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
			int partLimit = Integer.min(data.limit(), data.position() + leafNode.getSpace());
			ByteBuffer part = data.duplicate().limit(partLimit);
			writeNode(part, LEAF_NODE_INDEX, part.remaining());
			data.position(partLimit);
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
			final ByteBuffer bb = serializeNode(node);
			writeNode(bb, nodeIndex + 1, node.getExpandedLength());
			node.clearData();
		}
		node.write(buffer, expandedLength);

	}

	/**
	 * Create the root node.
	 *
	 * @throws InterruptedException In case action was interrupted during execution
	 * @throws ExecutionException   Thrown at execution
	 * @throws IOException
	 */
	private void createRoot() throws InterruptedException, ExecutionException, IOException {
		int nodeIndex = LEAF_NODE_INDEX;

		// Can the data be compressed in 1 node e.g. we have only data in the leaf node

		if (stackNodeList.size() == 2) {
			TreeNode leaf = stackNodeList.get(LEAF_NODE_INDEX);
			TreeNode inner = stackNodeList.get(FIRST_INNER_INDEX);
			if (inner.isDataEmpty() && inner.hasSpace(leaf.getUsedSpace())) {
				// create a reference to the root node
				final LeafNode leafNode = (LeafNode) stackNodeList.get(LEAF_NODE_INDEX);
				// Let's create our root node
				final TreeNode rootNode = new InnerNode(factory, leafNode);
				writeRoot(rootNode);
				return;
			}
		}

		TreeNode node = stackNodeList.get(nodeIndex);

		while (nodeIndex < stackNodeList.size() - 1) {
			final int nextPosition = nodeIndex + 1;
			final ByteBuffer bb = serializeNode(node);
			writeNode(bb, nextPosition, node.getExpandedLength());
			node.clearData();
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

		if (TreeOutputStream.LOG.isDebugEnabled()) {
			TreeOutputStream.LOG.debug("Writing node with space " + node.getSpace());
		}

		return serializer.serialize(serializer.serialize(node.getData()));

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
	protected void writeRoot(final TreeNode rootNode) throws ExecutionException, InterruptedException, IOException {

		if (TreeOutputStream.LOG.isDebugEnabled()) {
			TreeOutputStream.LOG.debug("Obtained data for the Root Node, writing it");
		}
		position = serializer.serialize(rootNode.getData());
		rootNode.clearData();
	}

	@Override
	public void close() throws IOException {
		super.close();
		closed = true;
		if (stackNodeList.get(LEAF_NODE_INDEX).isDataEmpty()) {
			position = serializer.getNoDataPosition();
		} else {
			try {
				createRoot();
			} catch (InterruptedException | ExecutionException ex) {
				throw new IOException("Failed at creating root node ", ex);
			}
		}
	}
}
