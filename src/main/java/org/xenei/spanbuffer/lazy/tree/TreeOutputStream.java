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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.xenei.spanbuffer.lazy.tree.node.TreeNode;
import org.xenei.spanbuffer.lazy.tree.serde.Position;
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
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
	private TreeSerializer serializer;
	private Position position;

	private static final Logger LOG = LoggerFactory.getLogger(TreeOutputStream.class);

	/*
	 * the stack of nodes we are writing to. Leaf is node 0, the rest are inner
	 * nodes leading back to the root node.
	 */
	private final List<TreeNode> stackNodeList;

	private static final int LEAF_NODE_INDEX = 0;
	private static final int FIRST_INNER_INDEX = 1;

	/**
	 * Constructor.
	 *
	 * @param serializer The TreeSerializer implementation to use for the output.
	 */
	public TreeOutputStream(@SuppressWarnings("rawtypes") TreeSerializer serializer) {

		this.serializer = serializer;
		this.position = serializer.getNoDataPosition();

		/* create the stack */
		stackNodeList = new ArrayList<>();
		stackNodeList.add(new LeafNode(serializer.getMaxBufferSize()));
		stackNodeList.add(new InnerNode(serializer.getMaxBufferSize(), InnerNode.LEAF_NODE_FLAG));
	}

	@Override
	public void write(final int arg0) throws IOException {

		if (closed) {
			throw new IOException("Stream is closed.");
		}

		try {
			writeNode(new byte[] { (byte) arg0 }, InnerNode.LEAF_NODE_FLAG, 1L);

		} catch (InterruptedException | ExecutionException ex) {
			ex.printStackTrace();
		}

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

		byte[] data = buffer;

		if (!((off == 0) && (len == buffer.length))) {
			data = Arrays.copyOfRange(buffer, off, len);
		}

		try {

			// Get the root node
			final TreeNode leafNode = stackNodeList.get(LEAF_NODE_INDEX);

			// check if we have space
			if (data.length <= leafNode.getSpace()) {
				writeNode(data, LEAF_NODE_INDEX, data.length);
			} else {

				/*
				 * IF not, chop the data into a chunk that would have enough data to fit the
				 * leaf node
				 */
				SpanBuffer node = Factory.wrap(data);

				if (leafNode.getOffset() > 0) {
					// Create an empty array with the remaining space left
					final byte[] bytes = new byte[(int) leafNode.getSpace()];

					node = startWritingToLeaf(node, bytes);
				}
				// The remaining data may still be bigger than the capacity of the span
				if (node.getLength() > leafNode.getSpan().getLength()) {

					// create an array equal to the length of the span
					final byte[] bytes = new byte[(int) leafNode.getSpan().getLength()];

					// write chunks of data (size = capacity)
					while (node.getLength() > leafNode.getSpan().getLength()) {
						node = startWritingToLeaf(node, bytes);
					}
				}

				// if we have the capacity for the data, then write it

				if (node.getLength() > 0) {
					final byte[] bytes = new byte[(int) node.getLength()];

					node.readRelative(0, bytes);

					writeNode(bytes, LEAF_NODE_INDEX, bytes.length);

				}

			}

		} catch (InterruptedException | ExecutionException ex) {
			ex.printStackTrace();
		}

	}

	private SpanBuffer startWritingToLeaf(final SpanBuffer node, final byte[] bytes)
			throws ExecutionException, InterruptedException, IOException {

		node.readRelative(0, bytes);

		writeNode(bytes, LEAF_NODE_INDEX, bytes.length);

		return node.cut(bytes.length);

	}

	/**
	 * Writes to node, the buffer must fin in an empty LeafNode.
	 *
	 * @param buffer         data buffer to be written in the node
	 * @param nodeIndex      index position in the stackNodeList
	 * @param expandedLength the length of the actual data
	 * @throws InterruptedException In case action was interrupted during execution
	 * @throws ExecutionException   Thrown at execution
	 *
	 */
	private void writeNode(final byte[] buffer, final int nodeIndex, final long expandedLength)
			throws InterruptedException, ExecutionException {

		// Node number should never be greater than the size
		if (nodeIndex >= stackNodeList.size()) {
			stackNodeList.add(new InnerNode(serializer.getMaxBufferSize(), InnerNode.INNER_NODE_FLAG));
		}

		final TreeNode node = stackNodeList.get(nodeIndex);

		if (!node.hasSpace(buffer.length)) {

			final ByteBuffer bb = writeNode(node);

			writeNode(bb.array(), nodeIndex + 1, node.getExpandedLength());

			node.clearData();
		}

		node.write(buffer, expandedLength);

	}

	/**
	 * When calling this buffer must fit in an empty LeafNode.
	 *
	 * @param nodeIndex Node position in the stackNodeList
	 * @throws InterruptedException In case action was interrupted during execution
	 * @throws ExecutionException   Thrown at execution
	 */
	private void createRoot(int nodeIndex) throws InterruptedException, ExecutionException {

		// We are in the last level of the tree - create root node
		if (nodeIndex >= stackNodeList.size()) {
			throw new IllegalStateException(
					String.format("Node Index is %s >  %s stackNodeList size", nodeIndex, stackNodeList.size()));
		}

		// Can the data be compressed in 1 node e.g. we have only data in the leaf node
		if ((nodeIndex == LEAF_NODE_INDEX) && (stackNodeList.size() == 2)
				&& stackNodeList.get(FIRST_INNER_INDEX).isDataEmpty()
				&& stackNodeList.get(FIRST_INNER_INDEX).hasSpace(InnerNode.FLAG_SIZE)) {

			// create a reference to the root node
			final LeafNode leafNode = (LeafNode) stackNodeList.get(LEAF_NODE_INDEX);
			// Let's create our root node
			final TreeNode rootNode = new InnerNode(leafNode);

			writeRoot(rootNode);

			return;
		}

		final TreeNode node = stackNodeList.get(nodeIndex);

		// Are we at the last node of the stack?
		if (nodeIndex == (stackNodeList.size() - 1)) {

			writeRoot(node);

		} else {

			// Go to next node
			final int nextPosition = ++nodeIndex;
			final ByteBuffer bb = writeNode(node);
			writeNode(bb.array(), nextPosition, node.getExpandedLength());
			node.clearData();
			createRoot(nextPosition);
		}
	}

	/**
	 * Once we write the root we can retrieve the Position information about the
	 * node.
	 * 
	 * @return Position object for the root
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
	 * Writes the node's data to the KafkaQueue and retrieves the newly obtained
	 * data (OPLImpl).
	 *
	 * @param node The tree node to serialize.
	 * @return the position encoded into a bytebuffer by the TreeSerializer.
	 * @throws InterruptedException In case action was interrupted during execution
	 * @throws ExecutionException   Thrown at execution
	 */
	@SuppressWarnings("unchecked")
	private ByteBuffer writeNode(final TreeNode node) throws InterruptedException, ExecutionException {

		if (TreeOutputStream.LOG.isDebugEnabled()) {
			TreeOutputStream.LOG.debug("Writing node with space " + node.getSpace());
		}

		return serializer.serialize(serializer.serialize(node.cloneData()));

	}

	/**
	 * Writes the data for the root node using the serializer. Updates the final
	 * position.
	 *
	 * @param rootNode The TreeNode that is the root node.
	 * @return Position information from TreeSerializer.
	 * @throws ExecutionException   on error
	 * @throws InterruptedException on error
	 */
	protected void writeRoot(final TreeNode rootNode) throws ExecutionException, InterruptedException {

		if (TreeOutputStream.LOG.isDebugEnabled()) {
			TreeOutputStream.LOG.debug("Obtained data for the Root Node, writing it");
		}
		position = serializer.serialize(rootNode.cloneData());
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
				createRoot(LEAF_NODE_INDEX);
			} catch (InterruptedException | ExecutionException ex) {
				throw new IOException("Failed at creating root node ", ex);
			}
		}
	}
}
