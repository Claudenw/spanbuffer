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

import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;

/**
 * This is the implementation of the span buffer on the tree structure.
 */
public abstract class AbstractNodeBuffer extends AbstractSpanBuffer {

	protected final int inset;
	private long bufferLength;
	@SuppressWarnings("rawtypes")
	protected final TreeLazyLoader lazyLoader;

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
	protected AbstractNodeBuffer(final long offset, final int inset, final long bufferLength,
			@SuppressWarnings("rawtypes") final TreeLazyLoader lazyLoader) {
		super(offset);
		this.inset = inset;
		this.bufferLength = lazyLoader.hasNoData() ? 0 : bufferLength;
		this.lazyLoader = lazyLoader;
	}

	/**
	 * Returns the delegate which in this case is the lazyLoader containing the
	 * information we need for the SpanBuffer.
	 *
	 * @return the delegating spanbuffer
	 * @throws IOException 
	 */
	protected abstract SpanBuffer getDelegate() throws IOException;

	@Override
	public final byte read(final long position) throws IOException {
		return getDelegate().read(position);
	}

	@Override
	public final int read(final long position, final byte[] buff, final int pos, final int len) throws IOException {
		return getDelegate().read(position, buff, pos, len);
	}

	@Override
	public final SpanBuffer sliceAt(final long position) {
		try {
			return getDelegate().sliceAt(position);
		} catch (IOException e) {
			throw new IllegalStateException( e );
		}
	}

	@Override
	public final long getEnd() {
		return LongSpan.calcEnd(this);
	}

	@Override
	public final long getLength() {
		if (bufferLength == LazyLoadedBuffer.UNDEF_LEN) {
			try {
				bufferLength = getDelegate().getLength();
			} catch (IOException e) {
				throw new IllegalStateException( e );
			}
		}
		return bufferLength - inset;
	}

	@Override
	public final SpanBuffer head(final long byteCount) {
		try {
			return getDelegate().head(byteCount);
		} catch (IOException e) {
			throw new IllegalStateException( e );
		}
	}

	@Override
	public final String toString() {
		StringBuilder sb;
		try {
			sb = new StringBuilder();
			sb.append(lazyLoader.toString());
			sb.append("_");
			sb.append(getDelegate().getText());

		} catch (final IOException ex) {
			sb = new StringBuilder();
			sb.append(lazyLoader.toString());
			sb.append("_");
			sb.append(inset);
			ex.printStackTrace();
		}
		return sb.toString();
	}

}
