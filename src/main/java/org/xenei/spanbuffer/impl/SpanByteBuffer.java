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
package org.xenei.spanbuffer.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xenei.span.NumberUtils;
import org.xenei.span.IntSpan;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A span that comprises a ByteBuffer. The logical beginning of the SpanBuffer
 * (offset) may be specified.
 */
public class SpanByteBuffer extends AbstractSpanBuffer {
	private final ByteBuffer buffer;
	/*
	 * This Span contains the inset values for the buffer. The offset is the
	 * position within the buffer that we start at and the end is the last byte to
	 * be included in the output.
	 */
	private final IntSpan span;

	/**
	 * Constructor.
	 *
	 * <p>
	 * Construct from a byte buffer. Uses an offset of 0.
	 * </p>
	 *
	 * @param buffer The byte buffer to use.
	 */
	public SpanByteBuffer(final ByteBuffer buffer) {
		this(0, buffer);
	}

	/**
	 * Constructor.
	 *
	 * <p>
	 * Construct from a byte buffer. The offset specifies the logical position of
	 * the first byte in the buffer.
	 * </p>
	 *
	 * @param offset the external offset for the buffer.
	 * @param buffer The byte buffer to use.
	 */
	public SpanByteBuffer(final long offset, final ByteBuffer buffer) {
		super(offset);
		this.buffer = buffer;
		this.span = IntSpan.fromEnd(buffer.position(), buffer.limit() - 1);
	}

	@Override
	public SpanByteBuffer duplicate(final long newOffset) {
		if (newOffset == getOffset()) {
			return this;
		}
		return new SpanByteBuffer(newOffset, buffer.duplicate());
	}

	@Override
	public SpanByteBuffer head(final long byteCount) {
		final int intLimit = NumberUtils.checkIntLimit("byteCount", byteCount);
		if ((intLimit < 0) || (intLimit > getLength())) {
			throw new IllegalArgumentException(
					String.format("byte count %s is not in the range [0,%s]", byteCount, getLength()));
		}
		final ByteBuffer newBuf = buffer.duplicate();
		newBuf.position(span.getOffset());
		newBuf.limit(span.getOffset() + intLimit);
		return new SpanByteBuffer(getOffset(), newBuf);
	}

	@Override
	public SpanBuffer sliceAt(final long position) {
		if (position == getOffset()) {
			return this;
		}
		if (position == (getOffset() + getLength())) {
			return Factory.EMPTY.duplicate(getOffset() + getLength());
		}
		final int checkInt = NumberUtils.checkIntLimit("position", localizePosition(position) + span.getOffset());
		buffer.position(checkInt);

		final SpanBuffer retval = new SpanByteBuffer(position, buffer.slice());
		buffer.position(span.getOffset());
		return retval;
	}

	@Override
	public byte read(final long position) throws IOException {
		final int intLimit = NumberUtils.checkIntLimit("position", localizePosition(position) + span.getOffset());
		return buffer.get(intLimit);
	}

	@Override
	public int read(final long position, final byte[] buff, final int pos, final int len) {
		final int intLimit = NumberUtils.checkIntLimit("position", localizePosition(position) + span.getOffset());
		buffer.position(intLimit);
		int bytesRead = Integer.min(len, buffer.remaining());
		buffer.get(buff, pos, bytesRead);
		buffer.position(span.getOffset());
		return bytesRead;
	}

	@Override
	public int read(final long position, final ByteBuffer buff) throws IOException {
		final int intLimit = NumberUtils.checkIntLimit("position", localizePosition(position) + span.getOffset());
		ByteBuffer bb = buffer.duplicate().position(intLimit);
		int limit = (bb.remaining() > buff.remaining()) ? buff.remaining() : bb.remaining();
		bb.limit(limit + intLimit);
		buff.put(bb);
		return limit;
	}

	@Override
	public long getLength() {
		return span.getLength();
	}

	@Override
	public long getEnd() {
		return LongSpan.calcEnd(this);
	}

}
