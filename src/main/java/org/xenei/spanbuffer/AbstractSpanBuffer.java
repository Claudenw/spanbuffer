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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.util.NoSuchElementException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.span.LongSpan;
import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;

/**
 * An abstract implementation of the span buffer.
 *
 * <p>
 * Provides common implementations for most methods.
 * </p>
 */
public abstract class AbstractSpanBuffer implements SpanBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractSpanBuffer.class);
	private final long offset;
	private Integer hashValue;

	/**
	 * Constructor.
	 *
	 * @param offset the Offset of this buffer.
	 */
	protected AbstractSpanBuffer(final long offset) {
		this.offset = offset;
		hashValue = null;
	}

	/**
	 * Consumes the needleWalker while attempting to match the hayWalker. On success
	 * the needleWalker.hasNext() will return false and hayWalker.getPos() will
	 * return the position of the match.
	 * <p>
	 * On failure sets the hayWalker back to starting position.
	 * </p>
	 *
	 * @param hayWalker    haystack where the niddle may be contained
	 * @param needleWalker needle to find
	 * @return true if the position was found, false otherwise.
	 */
	private static boolean subPositionOf(final Walker hayWalker, final Walker needleWalker) {
		final long hayPos = hayWalker.getPos();
		try {
			while (needleWalker.hasCurrent() && hayWalker.hasCurrent()) {

				if (hayWalker.getByte() == needleWalker.getByte()) {
					hayWalker.next();
					needleWalker.next();

					if (!needleWalker.hasCurrent()) {
						return true;
					}

				} else {
					return false;
				}
				/*
				 * we are looping until the match is not found or we get to end of needle.
				 */
			}
			return false;
		} catch (final IOException exception) {
			LOG.error("Error locating position -- returning false", exception);
			return false;
		} finally {
			hayWalker.setPosition(hayPos);
		}
	}

	@Override
	public SpanBuffer tail(final long byteCount) {
		if (byteCount == 0) {
			return Factory.EMPTY;
		}
		if (byteCount > getLength()) {
			throw new IllegalArgumentException("byte count may not be greater than " + getLength());
		}
		return sliceAt((getOffset() + getLength()) - byteCount);
	}

	@Override
	public SpanBuffer safeTail(final long position) {
		if (getLength() == 0) {
			return Factory.EMPTY;
		}
		return sliceAt(Math.max(getOffset(), (getEnd() - position) + 1));
	}

	@Override
	public SpanBuffer cut(final long byteCount) {
		if (byteCount == 0) {
			return this;
		}
		if (byteCount == getLength()) {
			return Factory.EMPTY.duplicate(byteCount + getOffset());
		}

		if (byteCount > getLength()) {
			throw new IllegalArgumentException("byte count may not exceed " + getLength());
		}
		return sliceAt(byteCount + getOffset());
	}

	@Override
	public SpanBuffer trunc(final long position) {
		return head(position - getOffset());
	}

	@Override
	public SpanBuffer concat(final SpanBuffer otherBuffer) {
		return Factory.merge(this, otherBuffer);
	}

	@Override
	public final long getOffset() {
		return offset;
	}

	@Override
	public final boolean equals(final Object other) {

		if (other instanceof SpanBuffer) {
			return SpanBuffer.Utils.equals(this, (SpanBuffer) other);
		}
		return false;
	}

	@Override
	public final int hashCode() {
		if (hashValue == null) {
			hashValue = SpanBuffer.Utils.hashCode(this);
		}
		return hashValue.intValue();
	}

	/**
	 * Convert the external position into the position in the buffer.
	 *
	 * @param position The external position
	 * @return The localized position.
	 */
	protected long localizePosition(final long position) {
		if (position < getOffset()) {
			throw new IllegalArgumentException(
					String.format("position (%s) must not be less than %s", position, getOffset()));
		}
		if (position > getEnd()) {
			throw new IllegalArgumentException(
					String.format("position (%s) must not be greater than %s", position, getEnd()));
		}
		return NumberUtils.checkLongAddLimit(position, -offset);

	}

	@Override
	public SpanBufferInputStream getInputStream() {
		return new SpanBufferInputStream(this);
	}

	@Override
	public long positionOf(final SpanBuffer needle, final long loc) throws NoMatchException {
		if (getLength() == 0) {
			throw new NoMatchException();
		}
		if (loc > getEnd()) {
			throw new NoSuchElementException(
					String.format("location %s is beyond the end of the buffer %s", loc, getEnd()));
		}
		if (needle.getLength() == 0) {
			return loc;
		}
		final Walker needleWalker = needle.getWalker();
		final Walker hayWalker = getWalker(loc);

		while (needleWalker.hasCurrent()) {
			if (AbstractSpanBuffer.subPositionOf(hayWalker, needleWalker)) {
				return hayWalker.getPos();
			}
			needleWalker.setPosition(needle.getOffset());
			// move the haystack pointer

			if ((getEnd() - needle.getLength()) >= hayWalker.getPos()) {
				hayWalker.next();
			} else {
				throw new NoMatchException();
			}
		}
		throw new NoMatchException();

	}

	@Override
	public long positionOf(final SpanBuffer needle) throws NoMatchException {
		return positionOf(needle, getOffset());
	}

	@Override
	public long lastPositionOf(final SpanBuffer needle, final long pos) throws NoMatchException {
		if (needle.getLength() == 0) {
			return Math.min(pos, getEnd());
		}
		final Walker needleWalker = needle.getWalker();
		final Walker hayWalker = this.getWalker(pos);
		while (hayWalker.hasCurrent()) {
			if (AbstractSpanBuffer.subPositionOf(hayWalker, needleWalker)) {
				return hayWalker.getPos();
			}
			needleWalker.setPosition(needle.getOffset());
			hayWalker.prev();
		}
		throw new NoMatchException();
	}

	@Override
	public long lastPositionOf(final SpanBuffer needle) throws NoMatchException {
		return lastPositionOf(needle, getEnd());
	}

	@Override
	public long commonPrefix(final SpanBuffer other) throws IOException {
		final Walker myWalker = getWalker(getOffset());
		final Walker otherWalker = other.getWalker(other.getOffset());
		while (myWalker.hasCurrent() && otherWalker.hasCurrent() && (myWalker.getByte() == otherWalker.getByte())) {
			myWalker.next();
			otherWalker.next();
		}

		return myWalker.getPos() - getOffset();
	}

	@Override
	public long commonSuffix(final SpanBuffer other) throws IOException {
		final Walker myWalker = getWalker(getEnd());
		final Walker otherWalker = other.getWalker(other.getEnd());

		while (myWalker.hasCurrent() && otherWalker.hasCurrent() && (myWalker.getByte() == otherWalker.getByte())) {
			myWalker.prev();
			otherWalker.prev();
		}

		return getEnd() - myWalker.getPos();
	}

	@Override
	public final byte readRelative(final long byteOffset) throws IOException {
		return read(getOffset() + byteOffset);
	}

	@Override
	public final int readRelative(final long byteOffset, final byte[] buff) throws IOException {
		return read(getOffset() + byteOffset, buff);
	}

	@Override
	public final int readRelative(final long byteOffset, final byte[] buff, final int pos, final int len)
			throws IOException {
		return read(getOffset() + byteOffset, buff, pos, len);
	}

	@Override
	public final int readRelative(final long byteOffset, final ByteBuffer buff)
			throws IOException {
		return read(getOffset() + byteOffset, buff);
	}
	@Override
	public final int read(final long position, final byte[] buff) throws IOException {
		return read(position, buff, 0, buff.length);
	}

	@Override
	public Walker getWalker() {
		return this.getWalker(getOffset());
	}

	@Override
	public Walker getWalker(final long position) {
		return new WalkerImpl(this, position);
	}

	@Override
	public String getText() throws IOException {
		final InputStream is = new SpanBufferInputStream(this);
		final char[] chars = IOUtils.toCharArray(is, "UTF-8");
		return String.valueOf(chars);
	}

	@Override
	public String getHex() throws IOException {
		return Hex.encodeHexString(IOUtils.toByteArray(new SpanBufferInputStream(this)));
	}

	@Override
	public String getHex(final int limit) throws IOException {
		final int len = (int) Math.min(Integer.MAX_VALUE, Math.min(getLength(), limit));
		if (len == 0) {
			return "";
		}
		final byte[] buff = new byte[len];
		try (InputStream is = new SpanBufferInputStream(this)) {
			final int count = is.read(buff, 0, len);
			if (count == len) {
				return Hex.encodeHexString(buff);
			} else {
				final byte[] buff2 = new byte[count];
				System.arraycopy(buff, 0, buff2, 0, count);
				return Hex.encodeHexString(buff2);
			}
		}
	}

	@Override
	public boolean startsWith(final SpanBuffer other) throws IOException {

		if (getLength() < other.getLength()) {
			return false;
		}
		final Walker myWalker = getWalker();
		for (final Walker otherWalker = other.getWalker(); otherWalker.hasCurrent(); otherWalker.next()) {
			if (myWalker.getByte() != otherWalker.getByte()) {
				return false;
			}
			myWalker.next();
		}
		return true;
	}

	@Override
	public boolean endsWith(final SpanBuffer other) throws IOException {
		final long offset = getLength() - other.getLength();
		if (offset < 0) {
			return false;
		}
		final Walker myWalker = getWalker(offset + getOffset());
		for (final Walker otherWalker = other.getWalker(); otherWalker.hasCurrent(); otherWalker.next()) {
			if (myWalker.getByte() != otherWalker.getByte()) {
				return false;
			}
			myWalker.next();
		}
		return true;
	}

	@Override
	public SpanBuffer safeSliceAt(final long position) {
		return contains(position) ? sliceAt(position) : Factory.EMPTY;
	}

	@Override
	public String toString() {
		return SpanBuffer.Utils.simpleText(this);
	}

	private static class WalkerImpl implements Walker {

		private final SpanBuffer sb;
		// the absolute position in the buffer that the walker is at.
		private long pos;

		/**
		 * Create a SpanBuffer walker.
		 *
		 * @param sb  the span buffer to walk.
		 * @param pos the absolute position to start at.
		 */
		private WalkerImpl(final SpanBuffer sb, final long pos) {
			this.sb = sb;
			this.pos = pos;
		}

		@Override
		public boolean hasCurrent() {
			return (pos <= sb.getEnd()) && (pos >= sb.getOffset());
		}

		@Override
		public long next() {
			if (pos <= sb.getEnd()) {
				return ++pos;
			}
			return pos;
		}

		@Override
		public long prev() {
			if (pos >= sb.getOffset()) {
				return --pos;
			}
			return pos;
		}

		@Override
		public long getPos() {
			return pos;
		}

		@Override
		public byte getByte() throws IOException {
			return sb.read(pos);
		}

		@Override
		public long remaining() {
			// use end because pos is between [start,end] not [1,length]
			return (sb.getEnd() - pos) + 1;
		}

		@Override
		public SpanBuffer getBuffer() {
			return sb;
		}

		@Override
		public void increment(final long numberOfBytes) throws NoSuchElementException {
			pos += numberOfBytes;
			if ((pos - sb.getOffset()) > sb.getLength()) {
				throw new NoSuchElementException(String.format("Position %s is after end (%s)", pos, sb.getEnd()));
			}
			if (pos < sb.getOffset()) {
				throw new NoSuchElementException(
						String.format("Position %s is before start (%s)", pos, sb.getOffset()));
			}
		}

		@Override
		public void setPosition(final long position) throws NoSuchElementException {
			pos = position;
			if (pos > sb.getEnd()) {
				throw new NoSuchElementException(
						String.format("Position %s is after end (%s)", position, sb.getOffset()));
			}
			if (pos < sb.getOffset()) {
				throw new NoSuchElementException(
						String.format("Position %s is before start (%s)", position, sb.getOffset()));
			}
		}

		@Override
		public String toString() {
			Byte tmpBype = null;
			try {
				tmpBype = hasCurrent() ? getByte() : null;
			} catch (final IOException ex) {
				tmpBype = null;
			}
			return String.format("Walker[ %s pos:%s char:%s", sb, pos, tmpBype);
		}

		@Override
		public boolean readBoolean() throws IOException {
			return (readByte() != 0);
		}

		@Override
		public byte readByte() throws IOException {
			if (!hasCurrent()) {
				throw new EOFException();
			}
			try {
				return getByte();
			} finally {
				next();
			}
		}

		@Override
		public char readChar() throws IOException {
			return (char) ((readByte() << 8) | (readByte() & 0xff));

		}

		@Override
		public double readDouble() throws IOException {
			return Double.longBitsToDouble(readLong());
		}

		@Override
		public float readFloat() throws IOException {
			return Float.intBitsToFloat(readInt());
		}

		@Override
		public void readFully(byte[] buff) throws IOException {
			readFully(buff, 0, buff.length);
		}

		@Override
		public void readFully(byte[] buff, int offset, int len) throws IOException {
			if (len > remaining()) {
				throw new EOFException();
			}
			for (int i = 0; i < len; i++) {
				buff[i] = readByte();
			}
		}

		@Override
		public int read(byte[] buff) throws IOException {
			return read(buff, 0, buff.length);
		}

		@Override
		public int read(byte[] buff, int offset, int len) throws IOException {
			if (remaining() <= 0) {
				return -1;
			}

			if (remaining() <= Integer.MAX_VALUE) {
				len = Math.min(len, (int) remaining());
			}
			final int bytesRead = getBuffer().read(getPos(), buff, offset, len);
			increment(bytesRead);
			return bytesRead;
		}

		@Override
		public int read(ByteBuffer buff) throws IOException {
			int read = sb.read( pos, buff );
			pos += read;
			return read;
 		}
		
		@Override
		public int readInt() throws IOException {
			return (((readByte() & 0xff) << 24) | ((readByte() & 0xff) << 16) | ((readByte() & 0xff) << 8)
					| (readByte() & 0xff));
		}

		@Override
		public String readLine() throws IOException {
			StringBuffer strb = new StringBuffer();

			while (hasCurrent()) {
				byte c = readByte();
				if (c == '\r' && hasCurrent()) {
					if (getByte() == '\n') {
						pos++;
					}
					break;
				}
				if (c == '\n')
					break;
				strb.append((char) c);
			}

			return strb.length() > 0 ? strb.toString() : "";
		}

		@Override
		public long readLong() throws IOException {
			return (((long) (readByte() & 0xff) << 56) | ((long) (readByte() & 0xff) << 48)
					| ((long) (readByte() & 0xff) << 40) | ((long) (readByte() & 0xff) << 32)
					| ((long) (readByte() & 0xff) << 24) | ((long) (readByte() & 0xff) << 16)
					| ((long) (readByte() & 0xff) << 8) | (readByte() & 0xff));
		}

		@Override
		public short readShort() throws IOException {
			return (short) ((readByte() << 8) | (readByte() & 0xff));
		}

		@Override
		public String readUTF() throws IOException {
			CharsetDecoder cd = StandardCharsets.UTF_8.newDecoder();
			byte[] buff = new byte[readUnsignedShort()];
			readFully(buff);
			return cd.decode(ByteBuffer.wrap(buff)).toString();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return 0xFF & readByte();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return (((readByte() & 0xff) << 8) | (readByte() & 0xff));
		}

		@Override
		public int skipBytes(int n) throws IOException {
			if (n < 0) {
				return 0;
			}
			if (pos + n <= sb.getEnd()) {
				pos += n;
				return n;
			}
			try {
				long l = LongSpan.fromEnd(pos, sb.getEnd()).getLength();
				return NumberUtils.checkIntLimit("n", l);
			} catch (IllegalArgumentException e) {
				throw new IOException(e);
			} finally {
				pos = sb.getEnd() + 1;
			}

		}

	}
}
