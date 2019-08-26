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
package org.xenei.spanbuffer;

import java.io.IOException;

import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;

/**
 * Interface that places a span on a buffer. Only bytes within the span will be
 * visible.
 *
 * <p>
 * Span buffer methods that take positional arguments tend to come int 2
 * flavors. One takes absolute position the other takes relative position.
 * </p>
 *
 * <p>
 * Absolute position is defined as buffer.getOffset()+relativePosition. Absolute
 * position has a range of (getOffset(),getOffset()+getLength())
 * </p>
 *
 * <p>
 * Relative position is the position within the buffer when visualized as a byte
 * buffer. Relative position has a range of (0,getLength())
 * </p>
 */
public interface SpanBuffer extends LongSpan {

	/**
	 * Return the relative version of the absolute value. Where the absolute
	 * position may be outside the range of the span. Negative value may be returned
	 * from this method.
	 *
	 * @param absolute the position to convert to relative.
	 * @return the relative equivalent of the absolute value.
	 */
	public default long makeRelative(final long absolute) {
		return absolute - getOffset();
	}

	/**
	 * Return the absolute version of the relative position.
	 *
	 * @param relative the relative postion to convert to absolute.
	 * @return the absolute version.
	 */
	public default long makeAbsolute(final long relative) {
		return relative + getOffset();
	}

	/**
	 * When the candidate is greater than the relative upper limit the upper limit
	 * is returned.
	 *
	 * @param candidate the candidate to check.
	 * @return the maximum value or the candidate if it is in range.
	 */
	public default long relativeUpperLimit(final long candidate) {
		return contains(makeAbsolute(candidate)) ? candidate : makeRelative(getEnd());
	}

	/**
	 * When the candidate is greater than the absolute upper limit the upper limit
	 * is returned.
	 *
	 * @param candidate the candidate to check.
	 * @return the maximum value or the candidate if it is in range.
	 */
	public default long absoluteUpperLimit(final long candidate) {
		return contains(candidate) ? candidate : getEnd();
	}

	/**
	 * When the candidate is smaller than zero then zero is returned, otherwise the
	 * candidate is returned.
	 *
	 * @param candidate the candidate to check.
	 * @return max of 0 or candidate.
	 */
	public default long relativeLowerLimit(final long candidate) {
		return candidate < 0 ? 0 : candidate;
	}

	/**
	 * When the candidate is smaller than the absolute lower limit then the absolute
	 * lower limit is returned, otherwise the candidate is returned.
	 *
	 * @param candidate the candidate to check.
	 * @return max of getOffset() or candidate.
	 */
	public default long absoluteLowerLimit(final long candidate) {
		return contains(candidate) ? candidate : getOffset();
	}

	/**
	 * Create a duplicate of this buffer. This is used in cases where we need to
	 * create a new span buffer with differing offset.
	 *
	 * @param newOffset new offset
	 * @return a duplicate of the current SpanBuffer with a new offset
	 */
	SpanBuffer duplicate(long newOffset);

	/**
	 * Create a new SpanBuffer by slicing at a specific location.
	 *
	 * <p>
	 * The offset of the new span will be position. The byte at position in the
	 * original buffer will the the first byte in the resulting buffer. The rest of
	 * the original buffer will be in the resulting buffer.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 *
	 * </p>
	 * <p>
	 * The relative version of this method is cut().
	 * </p>
	 *
	 * @param position the absolute position to slice at.
	 * @return A slice from the original buffer.
	 * @throws IllegalArgumentException if the position is not within the span.
	 * @see #cut
	 * @see #getOffset
	 * @see #getLength
	 */
	SpanBuffer sliceAt(long position);

	/**
	 * Like sliceAt() except the that byteCount is the number of bytes to cut off
	 * the front.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 *
	 * </p>
	 * <p>
	 * The absolute version of this method is sliceAt().
	 * </p>
	 *
	 * @param byteCount the number of bytes to cut off the front.
	 * @return the new buffer comprising the old buffer with byteCount bytes removed
	 *         from the front.
	 * @see #sliceAt
	 */
	SpanBuffer cut(long byteCount);

	/**
	 * Create a new SpanBuffer by truncating at the specific location. The location
	 * will be included as the last byte in the new buffer.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 *
	 * </p>
	 * <p>
	 * The relative version of this method is head().
	 * </p>
	 *
	 * @param position The absolute last position to include in the new buffer.
	 * @return A new SpanBuffer encompassing start through position.
	 * @see #head
	 */
	SpanBuffer trunc(long position);

	/**
	 * Create a new SpanBuffer taking the last byteCount bytes from the current
	 * buffer.
	 *
	 * <p>
	 * the offset of the new span will be the offset of the position specified by
	 * byteCount as determined from the start position of the current buffer.
	 * </p>
	 *
	 * @param byteCount the number of bytes to take counting from the end of the
	 *                  buffer.
	 * @return A slice from the original buffer.
	 * @throws IllegalArgumentException if the resulting position is not within the
	 *                                  span.
	 */
	SpanBuffer tail(long byteCount);

	/**
	 * Create a new SpanBuffer taking the first byteCount bytes from the current
	 * buffer.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 *
	 * </p>
	 * <p>
	 * The absolute version of this method is trunc().
	 *
	 * </p>
	 * <p>
	 * The offset of the new span will be the offset of this span.
	 * </p>
	 *
	 * @param byteCount the number of bytes to take.
	 * @return The new buffer.
	 * @see #trunc
	 */
	SpanBuffer head(long byteCount);

	/**
	 * Create a new SpanBuffer by appending the contents of otherBuffer to the end
	 * of this buffer.
	 *
	 * @param otherBuffer the buffer to concatenate to the end of this one.
	 * @return the new span buffer.
	 */
	SpanBuffer concat(SpanBuffer otherBuffer);

	/**
	 * Like tail() create a new SpanBuffer by slicing at a specific position from
	 * the end of the buffer the offset of the new span will be the offset of the
	 * position. The difference is that safeTail() will not allow l to move before
	 * the start of the buffer.
	 *
	 * @param position the position from the end to slice at.
	 * @return A slice from the original buffer.
	 */
	SpanBuffer safeTail(long position);

	/**
	 * Like sliceAt() except that if the position is outside the span and empty
	 * SpanBuffer is returned.
	 *
	 * @param position the position to slice at.
	 * @return A slice from the original buffer.
	 */
	SpanBuffer safeSliceAt(long position);

	/**
	 * Create an input stream that reads the SpanBuffer.
	 *
	 * @return An input stream on the buffer.
	 */
	SpanBufferInputStream getInputStream();

	/**
	 * Get the absolute position of the needle in the SpanBuffer starting at
	 * specified absolute location.
	 *
	 * <p>
	 * An empty needle must return loc.
	 *
	 * </p>
	 * <p>
	 * This is an absolute method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param needle The needle to search for.
	 * @param loc    The absolute position to start searching from.
	 * @return The absolute position of the needle in the haystack after loc.
	 * @throws NoMatchException if the needle is not found
	 */
	long positionOf(SpanBuffer needle, long loc) throws NoMatchException;

	/**
	 * Get the absolute position of the needle in the SpanBuffer.
	 *
	 * <p>
	 * An empty needle must match the first position.
	 *
	 * </p>
	 * <p>
	 * This is an absolute method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param needle The needle to search for.
	 * @return The first absolute position of the needle in the SpanBuffer.
	 * @throws NoMatchException if the needle is not found
	 */
	long positionOf(SpanBuffer needle) throws NoMatchException;

	/**
	 * Get the last absolute position of the needle in the SpanBuffer starting at
	 * specified absolute position. The result is a relative value as well.
	 *
	 * <p>
	 * An empty needle must return pos argument.
	 *
	 * </p>
	 * <p>
	 * This is a relative method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param needle The needle to search for.
	 * @param pos    The absolute position start searching from.
	 * @return The last relative position of the needle in the SpanBuffer before
	 *         pos.
	 * @throws NoMatchException if the needle is not found
	 */
	long lastPositionOf(SpanBuffer needle, long pos) throws NoMatchException;

	/**
	 * Get the last absolute position of the needle in the SpanBuffer. Result is
	 * relative to getOffset()
	 *
	 * <p>
	 * An empty needle must return the last position.
	 *
	 * </p>
	 * <p>
	 * This is a relative method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param needle The needle to search for.
	 * @return The last relative position of the needle in the SpanBuffer.
	 * @throws NoMatchException if the needle is not found
	 */
	long lastPositionOf(SpanBuffer needle) throws NoMatchException;

	/**
	 * Determine the common prefix with another SpanBuffer.
	 *
	 * @param buffer2 Second SpanBuffer.
	 * @return The number of bytes common to the start of each SpanBuffer.
	 * @throws IOException on IO error
	 */
	long commonPrefix(SpanBuffer buffer2) throws IOException;

	/**
	 * Determine the common suffix with another SpanBuffer.
	 *
	 * @param buffer2 Second SpanBuffer.
	 * @return The number of bytes common to the end of each SpanBuffer.
	 * @throws IOException on IO error
	 */
	long commonSuffix(SpanBuffer buffer2) throws IOException;

	/**
	 * Return the entire SpanBuffer as a string.
	 *
	 * @return The contents of the buffer as a UTF-8 String.
	 * @throws IOException on error.
	 */
	String getText() throws IOException;

	/**
	 * Return the entire SpanBuffer as a hex encoded string.
	 *
	 * @return the contents of the buffer as a hex encoded string.
	 * @throws IOException on error.
	 */
	String getHex() throws IOException;

	/**
	 * Return the part of the SpanBuffer as a hex encoded string.
	 *
	 * @param limit the maximum number of bytes to encode.
	 * @return the contents of the buffer as a hex encoded string.
	 * @throws IOException on error.
	 */
	String getHex(int limit) throws IOException;

	/**
	 * Get a matcher object on the span buffer.
	 *
	 * @return a Matcher
	 */
	Matcher getMatcher();

	/**
	 * Read the byte at the absolute specified position.
	 *
	 * <p>
	 * This is an absolute method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param position the absolute position to read from.
	 * @return the byte
	 * @throws IOException on error
	 * @see #readRelative(long)
	 */
	byte read(long position) throws IOException;

	/**
	 * Read the bytes from the specified absolute position into the provided buffer.
	 * Equivalent to calling <code>read(
	 * position, buff, 0, buff.length )</code>.
	 *
	 * <p>
	 * This is an absolute method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param position the absolute position to start read from.
	 * @param buff     the buffer to copy the bytes into.
	 * @return the number of bytes read. May be less then length
	 * @throws IOException on error
	 * @see #readRelative(long, byte[])
	 */
	int read(long position, byte[] buff) throws IOException;

	/**
	 * Read the bytes from the specified absolute position into the provided buffer.
	 *
	 * <p>
	 * This is an absolute method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param position the absolute position to start read from.
	 * @param buff     the buffer to copy the bytes into.
	 * @param pos      the position to copy the bytes into.
	 * @param len      the number of bytes to copy.
	 * @return the number of bytes read, may be less than len
	 * @throws IOException on error
	 * @see #readRelative(long, byte[], int, int)
	 */
	int read(long position, byte[] buff, int pos, int len) throws IOException;

	/**
	 * Read the byte at the specified relative position.
	 *
	 * <p>
	 * This is an relative method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param byteOffset the relative position to read from.
	 * @return the byte
	 * @throws IOException on error
	 * @see #read(long)
	 */
	byte readRelative(long byteOffset) throws IOException;

	/**
	 * Read the bytes from the specified relative position into the provided buffer.
	 * Equivalent to calling readRelative( position, buff, 0, buff.length );
	 *
	 * <p>
	 * This is an relative method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param byteOffset the relative position to begin read from.
	 * @param buff       the buffer to copy the bytes into.
	 * @return the number of bytes read. May be less then length
	 * @throws IOException on error
	 * @see #read(long, byte[])
	 */
	int readRelative(long byteOffset, byte[] buff) throws IOException;

	/**
	 * Read the bytes from the specified relative position into the provided buffer.
	 *
	 * <p>
	 * This is an relative method.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param byteOffset the relative position to begin read from.
	 * @param buff       the buffer to copy the bytes into.
	 * @param pos        the position to copy the bytes into.
	 * @param len        the number of bytes to copy.
	 * @return the number of bytes read, may be less than len
	 * @throws IOException on error
	 * @see #read(long, byte[], int, int)
	 */
	int readRelative(long byteOffset, byte[] buff, int pos, int len) throws IOException;

	/**
	 * Get a buffer walker starting at an absolute position..
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param position The absolute starting position within the buffer.
	 * @return The walker starting at the position.
	 */
	Walker getWalker(long position);

	/**
	 * Get a buffer walker starting at getOffset().
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @return The walker starting at first absolute position in the buffer.
	 */
	Walker getWalker();

	/**
	 * Does this buffer start with the other buffer.
	 *
	 * @param other the other buffer.
	 * @return true if the bytes at the start of this buffer are the same as from
	 *         the other buffer.
	 * @throws IOException on error
	 */
	boolean startsWith(SpanBuffer other) throws IOException;

	/**
	 * Does this buffer end with the other buffer.
	 *
	 * @param other the other buffer.
	 * @return true if the bytes at the end of this buffer are the same as from the
	 *         other buffer.
	 * @throws IOException on error
	 */
	boolean endsWith(SpanBuffer other) throws IOException;

	/**
	 * Class that contains reference implementations for various span buffer
	 * methods.
	 */
	class Utils {
		/**
		 * Default equality. Buffers are equal if they are the same length and have the
		 * same bytes in the same order. offset is not considered.
		 *
		 * @param one the first span buffer
		 * @param two the second span buffer
		 * @return true if the buffers are equal.
		 */
		public static boolean equals(final SpanBuffer one, final SpanBuffer two) {
			if (one.getLength() != two.getLength()) {
				return false;
			}
			final Walker walkerOne = one.getWalker(one.getOffset());
			final Walker walkerTwo = two.getWalker(two.getOffset());
			while (walkerOne.hasCurrent()) {
				try {
					if (walkerOne.getByte() != walkerTwo.getByte()) {
						return false;
					}
				} catch (final IOException ex) {
					return false;
				}
				walkerOne.next();
				walkerTwo.next();
			}
			return true;
		}

		/**
		 * Default hashCode. Hashcode is based solely on the contents of the buffer.
		 *
		 * @param one the span buffer to bulid the hash code for.
		 * @return the hash code
		 */
		public static int hashCode(final SpanBuffer one) {
			int hc = 1;
			final Walker walker = one.getWalker(one.getOffset());
			while (walker.hasCurrent()) {
				try {
					hc = (31 * hc) + walker.getByte();
				} catch (final IOException ex) {
					return hc;
				}
				walker.next();
			}
			return hc;
		}

		/**
		 * Convert the span buffer into a byte array.
		 *
		 * @param sb the span buffer
		 * @return The byte array.
		 * @throws IOException on IO error or if buffer length is greater than
		 *                     Integer.MAX_VALUE.
		 */
		public static byte[] asByteBuffer(final SpanBuffer sb) throws IOException {
			if (sb.getLength() > Integer.MAX_VALUE) {
				throw new IOException(String.format("Buffer length (%s) is greater thatn Integer.MAX_VALUE (%s)",
						sb.getLength(), Integer.MAX_VALUE));
			}
			final byte[] bytes = new byte[(int) sb.getLength()];
			sb.read(0, bytes);
			return bytes;
		}

		/**
		 * Return the buffer size and the first 10 bytes in hex.
		 *
		 * @param sb the span buffer to get simple text for.
		 * @return the string.
		 */
		public static String simpleText(final SpanBuffer sb) {
			try {
				return String.format("%s %s", LongSpan.toString(sb), sb.getHex(10));
			} catch (final IOException ex) {
				return String.format("%s %s", LongSpan.toString(sb), "unable to retrieve");
			}

		}
	}

}
