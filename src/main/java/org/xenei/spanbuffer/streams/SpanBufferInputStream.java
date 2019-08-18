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
package org.xenei.spanbuffer.streams;

import java.io.IOException;
import java.io.InputStream;

import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.SpanBuffer.Walker;

/**
 * An InputStream implementation on top of a span buffer. Multiple
 * SpanBufferInputStreams may be active on a single SpanBuffer at one time.
 */
public class SpanBufferInputStream extends InputStream {

	private final Walker walker;

	/**
	 * Constructor.
	 * 
	 * @param buf the span buffer to read from.
	 */
	public SpanBufferInputStream(final SpanBuffer buf) {
		walker = buf.getWalker(buf.getOffset());
	}

	/**
	 * get the number of bytes read.
	 * 
	 * @return the number of bytes read.
	 */
	public long getBytesRead() {
		return walker.getBuffer().makeRelative(walker.getPos());
	}

	/**
	 * Get the span buffer this input stream is reading from.
	 * 
	 * @return the enclosed span buffer.
	 */
	public SpanBuffer getSpanBuffer() {
		return walker.getBuffer();
	}

	@Override
	public int available() {
		return (int) Math.min(walker.remaining(), Integer.MAX_VALUE);
	}

	@Override
	public int read() throws IOException {
		if (!walker.hasCurrent()) {
			return -1;
		}
		try {
			return walker.getByte() & 0xFF;
		} finally {
			walker.next();
		}
	}

	@Override
	public int read(final byte[] bytes, final int off, int len) {
		if (walker.remaining() <= 0) {
			return -1;
		}

		if (walker.remaining() <= Integer.MAX_VALUE) {
			len = Math.min(len, (int) walker.remaining());
		}
		final int bytesRead = walker.getBuffer().read(walker.getPos(), bytes, off, len);
		walker.increment(bytesRead);
		return bytesRead;
	}
}