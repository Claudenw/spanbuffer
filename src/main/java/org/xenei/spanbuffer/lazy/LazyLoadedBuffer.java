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
package org.xenei.spanbuffer.lazy;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.xenei.span.NumberUtils;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * SpanBuffer implementation that wraps a LazyLoader.
 *
 * <p>
 * Because new instances of LazyLoadBuffer that wrap the same LazyLoader
 * instance are returned from methods like duplicate(), head(), sliceAt(), etc.
 * LazyLoadBuffer maintains some additional internal state that isn't required
 * for other implementations like
 * <ul>
 * <li>inset - a position analogous to the position property in ByteBuffer (used
 * internally by SpanByteBuffer)</li>
 * <li>bufferLength - the length of the lazy laoded buffer.</li>
 * </ul>
 * </p>
 */
public class LazyLoadedBuffer extends AbstractSpanBuffer {
	/**
	 * The position within the lazy loaded buffer that is our offset in the buffer.
	 * Compare to offset - the offset of the buffer with respect to an outside
	 * index.
	 */
	private final int inset;
	/**
	 * The length of the lazy loaded buffer.
	 */
	private long bufferLength;

	/**
	 * The lazy loader this span buffer is using.
	 */
	private final LazyLoader lazyLoader;

	/**
	 * the length of a buffer with the length is undefined or unknown.
	 */
	public static final int UNDEF_LEN = -1;

	/**
	 * Create a SpanLazyLoadBuffer with default offset.
	 *
	 * @param lazyLoader the lazy loader to load from.
	 */
	public LazyLoadedBuffer(final LazyLoader lazyLoader) {
		this(0L, lazyLoader);
	}

	/**
	 * if lazyLoader doesn't have a prepopulated length, construction of
	 * SpanLazyLoadBuffer will cause the lazyLoader to load its data.
	 *
	 * @param offset     The external offset for this buffer.
	 * @param lazyLoader the lazy loader.
	 */
	public LazyLoadedBuffer(final long offset, final LazyLoader lazyLoader) {
		this(offset, 0, lazyLoader);
	}

	/**
	 * Constructor.
	 *
	 * @param offset     The external offset for this buffer.
	 * @param inset      The offset into the internal buffer where this buffer
	 *                   starts.
	 * @param lazyLoader the lazy loader.
	 */
	private LazyLoadedBuffer(final long offset, final int inset, final LazyLoader lazyLoader) {
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
	 * @param lazyLoader   the lazy loader.
	 */
	private LazyLoadedBuffer(final long offset, final int inset, final long bufferLength, final LazyLoader lazyLoader) {
		super(offset);
		this.inset = inset;
		this.bufferLength = bufferLength;
		this.lazyLoader = lazyLoader;
	}

	@Override
	public SpanBuffer duplicate(final long newOffset) {
		if (newOffset == getOffset()) {
			return this;
		}
		return new LazyLoadedBuffer(newOffset, inset, bufferLength, lazyLoader);
	}

	@Override
	public SpanBuffer sliceAt(final long position) {
		if (position == getOffset()) {
			return this;
		}
		if (position == (getOffset() + getLength())) {
			return Factory.EMPTY.duplicate(getOffset() + getLength());
		}
		final int newInset = NumberUtils.checkIntLimit("position", localizePosition(position)) + inset;

		return new LazyLoadedBuffer(position, newInset, bufferLength, lazyLoader);
	}

	@Override
	public SpanBuffer head(final long byteCount) {
		final int intLimit = NumberUtils.checkIntLimit("byteCount", byteCount);
		if ((intLimit < 0) || (intLimit > getLength())) {
			throw new IllegalArgumentException(
					String.format("byte count %s is not in the range [0,%s]", byteCount, getLength()));
		}
		return new LazyLoadedBuffer(getOffset(), inset, intLimit + inset, lazyLoader);
	}

	@Override
	public byte read(final long position) throws IOException {
		final int intLimit = NumberUtils.checkIntLimit("position", localizePosition(position));
		return lazyLoader.getBuffer()[intLimit + inset];
	}

	@Override
	public int read(final long position, final byte[] buff, final int pos, final int len) {
		final int intLimit = NumberUtils.checkIntLimit("position", localizePosition(position));
		final ByteBuffer buffer = ByteBuffer.wrap(lazyLoader.getBuffer());

		buffer.position(inset + intLimit);
		try {
			buffer.get(buff, pos, len);
			return len;
		} catch (final BufferUnderflowException ex) {
			final int bytesRead = buffer.remaining();
			buffer.get(buff, pos, bytesRead);
			return bytesRead;
		}
	}

	@Override
	public long getLength() {
		if (bufferLength == LazyLoadedBuffer.UNDEF_LEN) {
			bufferLength = lazyLoader.getLength();
		}
		return bufferLength - inset;
	}

	@Override
	public long getEnd() {
		return LongSpan.calcEnd(this);
	}

}
