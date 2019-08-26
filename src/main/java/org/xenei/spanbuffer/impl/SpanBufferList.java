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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A SpanBufferList is an SpanBuffer which comprises of multiple SpanBuffer
 * objects. This allows creation of SpanBuffers that take bits and pieces from
 * other SpanBuffers and make them all act as one SpanBuffer.
 */
public class SpanBufferList extends AbstractSpanBuffer {

	// the list of the buffers that comprise this buffer.
	private final LinkedList<SpanBuffer> spanList = new LinkedList<>();
	// the length of this span buffer
	private long length;

	/**
	 * Constructor. Create a SpanBufferList with an offset specified by the offset
	 * parameter. The SpanBuffers listed in the spans will be concatenated to create
	 * one SpanBuffer starting from offset and having a length equal to the total
	 * length of the buffers in the spans argument.
	 *
	 * @param offset the offset for the first byte of this SpanBuffer.
	 * @param spans  An array of SpanBuffer that will be assembled into a single
	 *               coherent buffer.
	 */
	public SpanBufferList(final long offset, final Collection<SpanBuffer> spans) {
		super(offset);

		final Iterator<SpanBuffer> iter = spans.stream().filter(span -> span.getLength() != 0).iterator();

		long pos = offset;
		SpanBuffer mySpan;
		while (iter.hasNext()) {
			mySpan = iter.next().duplicate(pos);
			spanList.add(mySpan);
			length += mySpan.getLength();
			pos = mySpan.getEnd() + 1;
		}
	}

	@Override
	public SpanBuffer duplicate(final long newOffset) {
		if (spanList.isEmpty()) {
			return Factory.EMPTY.duplicate(newOffset);
		}
		if (spanList.size() == 1) {
			return spanList.getFirst().duplicate(newOffset);
		}
		return new SpanBufferList(newOffset, spanList);
	}

	@Override
	public SpanBuffer head(final long byteCount) {
		if (byteCount == 0) {
			return Factory.EMPTY;
		}
		if (spanList.size() == 1) {
			return spanList.getFirst().head(byteCount);
		}
		final List<SpanBuffer> buffLst = new ArrayList<>();
		int offset = 0;

		for (final SpanBuffer sb : spanList) {
			if ((offset + sb.getLength()) < byteCount) {
				offset += sb.getLength();
				buffLst.add(sb);
			} else {
				buffLst.add(sb.head(byteCount - offset));
				return new SpanBufferList(getOffset(), buffLst);
			}
		}

		return new SpanBufferList(getOffset(), spanList);
	}

	/**
	 * Locate the SpanBuffer in the list that contains the position.
	 *
	 * @param position the position to locate
	 * @return the SpanBuffer containing the position or null if not found.
	 */
	private SpanBuffer locateSpan(final long position) {
		// find the first instance or null
		return spanList.stream().filter(sb -> sb.contains(position)).findFirst().orElse(null);
	}

	/**
	 * Locate the index of the SpanBuffer in the list tht contains the position.
	 *
	 * @param position The position to locate.
	 * @return The index of the SpanBuffer containing the position or -1 if not
	 *         found.
	 */
	private int locateSpanIndex(final long position) {

		for (int i = 0; i < spanList.size(); i++) {
			if (spanList.get(i).contains(position)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public SpanBuffer sliceAt(final long position) {
		if (position == getOffset()) {
			return this;
		}
		if (position == (getOffset() + getLength())) {
			return Factory.EMPTY.duplicate(getOffset() + getLength());
		}
		final int idx = locateSpanIndex(position);
		if (idx == -1) {
			throw new IllegalArgumentException(String.format("Position %s not in buffer %s", position, this));
		}
		final SpanBuffer sb = spanList.get(idx);
		final List<SpanBuffer> newList = new ArrayList<>();
		newList.add(sb.sliceAt(position));
		for (int i = idx + 1; i < spanList.size(); i++) {
			newList.add(spanList.get(i));
		}
		return Factory.merge(position, newList.iterator());
	}

	@Override
	public byte read(final long position) throws IOException {
		final SpanBuffer sb = locateSpan(position);
		if (sb == null) {
			throw new IOException(String.format("Position %s not in buffer", position));
		}
		return sb.read(position);
	}

	@Override
	public int read(final long position, final byte[] buff, final int pos, final int len) throws IOException {
		final SpanBuffer sb = locateSpan(position);
		if (sb == null) {
			return 0;
		}
		int bytesRead = sb.read(position, buff, pos, len);
		if (bytesRead != len) {
			// recurse to read more
			final int newPos = pos + bytesRead;
			final int newLen = len - bytesRead;
			final long newPosition = position + bytesRead;
			final int nextRead = this.read(newPosition, buff, newPos, newLen);
			bytesRead += nextRead;
		}
		return bytesRead;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public long getEnd() {
		return LongSpan.calcEnd(this);
	}

}
