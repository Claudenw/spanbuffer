package org.xenei.spanbuffer;

import java.io.DataInput;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An object that walks up or down the spanned buffer.
 *
 * <p>
 * Could be used in a for loop like:
 * </p>
 * <p>
 * <code>
 * for (Walker w=spanBuffer.getWalker(0);walker.hasCurrent();walker.next())
 * </code>
 * </p>
 */
public interface Walker extends DataInput {
	/**
	 * Move to the next absolute position.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @return the new absolute position;
	 */
	long next();

	/**
	 * Move to the previous position.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @return the new absolute position;
	 */
	long prev();

	/**
	 * Verify there is a current position.
	 *
	 * @return true if the current position is valid, false otherwise.
	 */
	boolean hasCurrent();

	/**
	 * get the current absolute position value.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @return the current absolute position in the buffer.
	 */
	long getPos();

	/**
	 * Get the byte at the current position.
	 *
	 * @return the byte at the current position.
	 * @throws IOException on IO error
	 */
	byte getByte() throws IOException;

	/**
	 * Get the number of bytes remaining (after current position).
	 *
	 * @return the number of bytes remaining.
	 */
	long remaining();

	/**
	 * Get the span buffer this walker is walking.
	 *
	 * @return the span buffer this walker is walking.
	 */
	SpanBuffer getBuffer();

	/**
	 * Increment the position by number of bytes.
	 *
	 * @param numberOfBytes the number of bytes to move the position by.
	 * @throws NoSuchElementException if the numberOfBytes moves past the end of the
	 *                                buffer
	 */
	void increment(long numberOfBytes) throws NoSuchElementException;

	/**
	 * Set the absolute position of the walker on the buffer.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param position the absolute position to set to.
	 * @throws NoSuchElementException If the position is invalid.
	 */
	void setPosition(long position) throws NoSuchElementException;

	/**
	 * Attempt to fill the buffer.
	 * 
	 * @param buff the buffer to fill.
	 * @return the number of bytes read.
	 * @throws IOException on error
	 */
	int read(byte[] buff) throws IOException;

	/**
	 * Attempt to fill the buffer from offset and for length.
	 * 
	 * @param buff   the buffer to fill.
	 * @param offset the offset to start filling at.
	 * @param len    the number of bytes to read.
	 * @return the number of bytes read.
	 * @throws IOException on error
	 */
	int read(byte[] buff, int offset, int len) throws IOException;
}