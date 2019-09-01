package org.xenei.spanbuffer.lazy.tree.node;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BufferFactory {
	/**
	 * The minimum size required for a buffer.
	 */
	public static final int MIN_BUFFER_SIZE = Integer.BYTES+(2*Long.BYTES);

	/**
	 * Get the size of the buffers produced by this factory.
	 * @return the size of the buffers produced by this factory.
	 */
	int bufferSize();
	
	/**
	 * The number of bytes at the start of the buffer used by the 
	 * underlying block system.
	 * @return the number of bytes in the block header.
	 */
	int headerSize();
	
	/**
	 * Create a new buffer.
	 * @return a ByteBuffer to write the data to. 
	 * @throws IOException on error
	 */
	ByteBuffer createBuffer() throws IOException;

	/**
	 * Frees any references held by the buffer. (i.e. remove from memory mapped file)
	 * @param buffer the buffer to remove.
	 * @throws IOException on error.
	 */
	void free(ByteBuffer buffer) throws IOException;

}
