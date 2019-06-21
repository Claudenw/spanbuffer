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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;

/**
 * The implementation of a span buffer wrapping a lazily loaded leaf node.
 *
 */
public class LeafBuffer extends AbstractNodeBuffer {

    private static final Logger LOG = LoggerFactory.getLogger( LeafBuffer.class );
	private SpanBuffer delegate;
	

	/**
	 * Creates TreeNode with offset to 0.
	 *
	 * @param lazyLoader the lazy loader for the data.
	 */
	public LeafBuffer(@SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
		this(0L, lazyLoader);		
	}

	/**
	 * Creates a tree node with specified offset.
	 * @param offset the offset.
	 * @param lazyLoader the lazy loader for the data.
	 */
	public LeafBuffer(final long offset, @SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
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
	private LeafBuffer(final long offset, final int inset, @SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
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
			@SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {	
		super(offset,inset, bufferLength, lazyLoader);
		
	}

	@Override
	protected SpanBuffer getDelegate() {

		// Check if we need to create the delegate
		if (delegate != null) {
			if (LeafBuffer.LOG.isDebugEnabled()) {
				LeafBuffer.LOG.debug("Returning delegate.");
			}
			return delegate;

		} else {
			delegate = Factory.wrap( getOffset(), lazyLoader.getBuffer());
		}

		return delegate;

	}

	@Override
	public SpanBuffer duplicate(final long newOffset) {
		return new LeafBuffer(newOffset, inset, getLength(), lazyLoader);
	}


}
