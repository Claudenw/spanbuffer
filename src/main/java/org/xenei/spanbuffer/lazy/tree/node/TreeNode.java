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
package org.xenei.spanbuffer.lazy.tree.node;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.span.IntSpan;

/**
 * Abstract node for tracking data in a tree buffer structure.
 *
 */
public abstract class TreeNode {
	// Logger
	private static final Logger LOG = LoggerFactory.getLogger(TreeNode.class);

	/**
	 * the span for this node. The offset is the position in the buffer where the
	 * dataspace starts. length is the length of the dataspace, and end is the end
	 * of the dataspace.
	 */
	protected final IntSpan span;

	// Data of the data
	protected ByteBuffer data;

	protected final BufferFactory factory;

	/**
	 * Constructor.
	 *
	 * @param factory the Factory to create buffers with.
	 * @throws IOException on error
	 */
	public TreeNode(BufferFactory factory) throws IOException {
		this.factory = factory;
		span = IntSpan.fromLength(factory.headerSize(), factory.bufferSize());
		data = factory.createBuffer();
	}

	/**
	 * Checks if it can write to the data buffer.
	 *
	 * @param bytes the number of bytes required in the buffer.
	 * @return true = Buffer has the capacity
	 */
	public boolean hasSpace(final int bytes) {
		return span.getLength() >= (data.position() + bytes);
	}

	/**
	 * Is the buffer empty? This varies as inner nodes always have a byte written to
	 * the buffer even if there is no 'real' data. This will return true if there is
	 * no 'real' data.
	 *
	 * @return true if there is no data in the buffer.
	 */
	public abstract boolean isDataEmpty();

	/**
	 * Writes data to the buffer. The expanded length is the number of bytes
	 * represented by the data to store. In leaf nodes this is the same as the dts
	 * length, for inner nodes the dts is probably an encoded Position and may
	 * represent many more bytes when the position is expanded.
	 *
	 * @param buff           Data to store.
	 * @param expandedLength length of the actual data represented by the data to
	 *                       store.
	 * @throws IllegalStateException if the data to store will not fit in the
	 *                               buffer.
	 */
	public final void write(ByteBuffer buff, final long expandedLength) {
		if (hasSpace(buff.limit() - buff.position())) {
			if (TreeNode.LOG.isTraceEnabled()) {
				TreeNode.LOG.trace(String.format("Writing to buffer at offset: %d and expanded length %d",
						data.position(), expandedLength));
			}
			data.put(buff);
			adjustLength(expandedLength);

		} else {
			throw new IllegalStateException("Attempted to write to full buffer");
		}

	}

	/**
	 * Writes data to the buffer. The expanded length is the number of bytes
	 * represented by the data to store. In leaf nodes this is the same as the dts
	 * length, for inner nodes the dts is probably an encoded Position and may
	 * represent many more bytes when the position is expanded.
	 *
	 * @param b              the byte to write.
	 * @param expandedLength length of the actual data represented by the data to
	 *                       store.
	 * @throws IllegalStateException if the data to store will not fit in the
	 *                               buffer.
	 */
	public final void write(byte b, final long expandedLength) {

		if (hasSpace(1)) {
			if (TreeNode.LOG.isTraceEnabled()) {
				TreeNode.LOG.trace(String.format("Writing to buffer at offset: %d and expanded length %d",
						data.position(), expandedLength));
			}
			data.put(b);
			adjustLength(expandedLength);

		} else {
			throw new IllegalStateException("Attempted to write to full buffer");
		}

	}

	/**
	 * Get the span for the actual byte buffer we are writing to.
	 * 
	 * @return the Span for the byte buffer.
	 */
	public IntSpan getSpan() {
		return span;
	}

	/**
	 * Adjust the length property node. For leaf nodes this method does nothing. For
	 * leaf inner nodes it increments the length that the buffer represents.
	 *
	 * @param addedLength the number of bytes to add to the length.
	 */
	protected abstract void adjustLength(long addedLength);

	/**
	 * Returns only the filled data buffer.
	 * 
	 * @return the filled data buffer.
	 */
	public ByteBuffer getData() {
		return data.duplicate().flip();
	}

	/**
	 * Get the raw byte buffer
	 * 
	 * @return the raw data buffer.
	 */
	public ByteBuffer getRawBuffer() {
		return data;
	}

	/**
	 * Set the data buffer back to its initial state. The number of free bytes
	 * should be the same as when the constructor was called. This method should
	 * allocate a new buffer
	 * 
	 * @throws IOException
	 */
	public abstract void clearData() throws IOException;

	/**
	 * Expanded length of the block. For the leaf node this is the length of the
	 * buffer. For inner nodes this is the number of bytes encompassed by all the
	 * leaf nodes the inner nodes point to.
	 *
	 * @return length of the overall block
	 */
	public abstract long getExpandedLength();

	/**
	 * Retrieves the space left in the buffer.
	 *
	 * @return space left in the buffer.
	 */
	public int getSpace() {
		return data.remaining();
	}

	/**
	 * Get the number of bytes in the buffer
	 * 
	 * @return the number of bytes in the buffer.
	 */
	public int getUsedSpace() {
		return data.position() - span.getOffset();
	}

}