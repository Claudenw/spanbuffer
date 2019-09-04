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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.lazy.tree.serde.Position;

/**
 * The implementation of a span buffer wrapping a lazily loaded leaf node.
 *
 */
public class LeafBuffer<P extends Position> extends AbstractNodeBuffer<P> {

	private static final Logger LOG = LoggerFactory.getLogger(LeafBuffer.class);
	private SpanBuffer delegate;

	/**
	 * Creates TreeNode with offset to 0.
	 *
	 * @param lazyLoader the lazy loader for the data.
	 */
	public LeafBuffer(final TreeLazyLoader<P> lazyLoader) {
		this(0L, lazyLoader);
	}

	/**
	 * Creates a tree node with specified offset.
	 * 
	 * @param offset     the offset.
	 * @param lazyLoader the lazy loader for the data.
	 */
	public LeafBuffer(final long offset, final TreeLazyLoader<P> lazyLoader) {
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
	private LeafBuffer(final long offset, final int inset, final TreeLazyLoader<P> lazyLoader) {
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
	private LeafBuffer(final long offset, final int inset, final long bufferLength,
			final TreeLazyLoader<P> lazyLoader) {
		super(offset, inset, bufferLength, lazyLoader);

	}

	@Override
	protected SpanBuffer getDelegate() throws IOException {

		// Check if we need to create the delegate
		if (delegate != null) {
			if (LeafBuffer.LOG.isDebugEnabled()) {
				LeafBuffer.LOG.debug("Returning delegate.");
			}
			return delegate;

		} else {
			delegate = lazyLoader.getRawBuffer(0).duplicate(getOffset());
		}

		return delegate;

	}

	@Override
	public SpanBuffer duplicate(final long newOffset) {
		return new LeafBuffer<P>(newOffset, inset, getLength(), lazyLoader);
	}

	@Override
	public String toString() {
		return String.format("LeafBuffer[ %s d:%s ]", getNodeBufferString(), delegate);
	}

}
