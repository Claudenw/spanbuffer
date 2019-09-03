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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;

/**
 * The implementation of the inner node buffer.
 *
 * This is a buffer that wraps an lazily loaded inner node.
 *
 */
public class InnerBuffer extends AbstractNodeBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(InnerBuffer.class);
	private SpanBuffer delegate;

	/**
	 * Creates TreeNode with offset to 0.
	 *
	 * @param lazyLoader the lazy loader for the data.
	 */
	@SuppressWarnings("rawtypes")
	public InnerBuffer(final TreeLazyLoader lazyLoader) {
		this(0L, lazyLoader);
	}

	/**
	 * Creates a tree node with specified offset.
	 * 
	 * @param offset     the offset.
	 * @param lazyLoader the lazy loader for the data.
	 */
	public InnerBuffer(final long offset, @SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
		this(offset, 0, lazyLoader);
	}

	/**
	 * Constructor.
	 *
	 * @param offset     The external offset for this buffer.
	 * @param inset      The offset into the internal buffer where this buffer
	 *                   starts.
	 * @param lazyLoader the lazy loader for the data
	 */
	private InnerBuffer(final long offset, final int inset,
			@SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
		this(offset, inset, LazyLoadedBuffer.UNDEF_LEN, lazyLoader);
	}

	/**
	 * Constructor.
	 *
	 * @param offset       The external offset for this buffer.
	 * @param inset        The offset into the internal buffer where this buffer
	 *                     starts.
	 * @param bufferLength the length of the internal buffer or UNDEF_LEN if
	 *                     unknown.
	 * @param lazyLoader   the lazy loader for the data
	 */
	private InnerBuffer(final long offset, final int inset, final long bufferLength,
			@SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
		super(offset, inset, bufferLength, lazyLoader);

	}

	/**
	 * Returns the delegate which in this case is the lazyLoader containing the
	 * information we need for the SpanBuffer.
	 *
	 * @return the delegate spanbuffer
	 * @throws IOException
	 */
	@Override
	protected SpanBuffer getDelegate() throws IOException {

		// Check if we need to create the delegate
		if (delegate != null) {
			if (InnerBuffer.LOG.isDebugEnabled()) {
				InnerBuffer.LOG.debug("Returning delegate.");
			}

			return delegate;

		} else {
			SpanBuffer buffer = lazyLoader.getBuffer(inset);
			if (buffer.getLength() == 0) {
				throw new IllegalStateException("Buffer must contain at least 1 byte");
			}

			/* Figure out what type of data we have */
			final byte typeFlag = buffer.read(InnerNode.FLAG_BYTE);
			
			final boolean innerNodePtrs = (typeFlag & InnerNode.INNER_NODE_FLAG) != 0;

			// cut the flag_byte from the front
			final SpanBuffer spanBuffer = buffer.cut(1);

			if (InnerBuffer.LOG.isDebugEnabled()) {
				InnerBuffer.LOG.debug("Generating Delegate, inner Nodes:[" + innerNodePtrs + "] ");
			}

			if (innerNodePtrs) {
				delegate = extract(spanBuffer);
			} else {

				final boolean outerNodeType = (typeFlag & InnerNode.OUTER_NODE_FLAG) != 0;

				if (!outerNodeType) {
					delegate = buildLeaves(spanBuffer);
				} else {
					// duplicating spanbuffer with new offset
					delegate = spanBuffer.duplicate(0);
				}

			}

		}

		return delegate;

	}

	/**
	 * Expand the SpanBuffer into multiple SpanBuffers by defeferencing the pointers
	 * into lazy loaders.
	 *
	 * @param buffer buffer to be expanded
	 * @return Expanded SpanBuffer
	 * @throws IOException on error
	 */
	private SpanBuffer extract(final SpanBuffer buffer) throws IOException {

		@SuppressWarnings({ "rawtypes", "unchecked" })
		final List<TreeLazyLoader> configs = lazyLoader.applyMap(buffer);

		if (InnerBuffer.LOG.isDebugEnabled()) {
			InnerBuffer.LOG.debug("Expanding Delegate, configs size:[" + configs.size() + "] ");
		}

		// to ensure that the offsets of the merged nodes are set correctly use
		// getOffset()
		return Factory.merge(getOffset(), configs.stream().map(InnerBuffer::new));

	}

	/**
	 * Builds a leaf node - at this stage the recursion will have found the
	 * end-node.
	 *
	 * @param buffer SpanBuffer to use for creating the leaf node.
	 * @return Leaf Node
	 * @throws IOException on error
	 */
	private SpanBuffer buildLeaves(final SpanBuffer buffer) throws IOException {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		final List<TreeLazyLoader> configs = lazyLoader.applyMap(buffer);

		if (InnerBuffer.LOG.isDebugEnabled()) {
			InnerBuffer.LOG.debug("Expanding Leaf Node, configs size:[" + configs.size() + "] ");
		}

		// to ensure that the offsets of the merged nodes are set correctly use
		// getStart()
		final SpanBuffer sb = Factory.merge(getOffset(), configs.stream().map(LeafBuffer::new));

		return sb;
	}

	@Override
	public SpanBuffer duplicate(final long newOffset) {
		return new InnerBuffer(newOffset, inset, getLength(), lazyLoader);
	}

}
