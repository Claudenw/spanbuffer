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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.impl.SpanBufferList;
import org.xenei.spanbuffer.impl.SpanByteBuffer;
import org.xenei.spanbuffer.lazy.AbstractLazyLoader;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

/**
 * A factory to create spanbuffers and to merge them
 *
 */
public final class Factory {

	/**
	 * The default maximum memory buffer size.
	 */
	private static long MAX_MEM_BUFFER = 32 * FileUtils.ONE_MB;

	/**
	 * Sets the maximum memory buffer size used when creating buffers from files or
	 * input streams. If the total size is greater than max mem buffer a memory
	 * mapped file buffer is used otherwise the data is read into a standard memory
	 * buffer. Initial default value is 500MB.
	 *
	 * @param newLimit The new memory limit
	 * @return the old memory limit
	 */
	public static long setMaxMemBuffer(final long newLimit) {
		final long retval = Factory.MAX_MEM_BUFFER;
		Factory.MAX_MEM_BUFFER = newLimit;
		return retval;
	}

	/**
	 * Gets the maximum memory buffer size used when creating buffers from files or
	 * input streams. If the total size is greater than max mem buffer a memory
	 * mapped file buffer is used otherwise the data is read into a standard memory
	 * buffer. Initial default value is 500MB.
	 *
	 * @return the memory limit
	 */
	public static long getMaxMemBuffer() {
		return Factory.MAX_MEM_BUFFER;
	}

	/**
	 * An empty span buffer. This instance may be returned anytime an empty buffer
	 * is needed.
	 */
	public static final SpanBuffer EMPTY = Factory.wrap(ByteBuffer.allocate(0));

	/**
	 * Wrap a ByteBuffer. Sets the offset (start) to 0 (zero). Any change to the
	 * underlying byte buffer will be reflected in the SpanBuffer. This is not
	 * recommended.
	 *
	 * @param bb the ByteBuffer to wrap.
	 * @return A span buffer wrapping the byte buffer.
	 */
	public static SpanBuffer wrap(final ByteBuffer bb) {
		return new SpanByteBuffer(bb);
	}

	/**
	 * Wrap a ByteBuffer and set the offset. Any change to the underlying byte
	 * buffer will be reflected in the SpanBuffer. This is not recommended.
	 *
	 * @param offset The offset that the SpanBuffer starts at.
	 * @param bb     the ByteBuffer to wrap.
	 * @return A span buffer wrapping the byte buffer.
	 */
	public static SpanBuffer wrap(final long offset, final ByteBuffer bb) {
		return new SpanByteBuffer(offset, bb);
	}

	/**
	 * Create a span buffer from a file.
	 *
	 * @param file The file to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final FileChannel fileChannel) throws FileNotFoundException, IOException {
		if (fileChannel == null) {
			throw new IllegalArgumentException("FileChannel must not be a null");
		}

		final MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

		buffer.load();
		// set position to zero and set the limit
		buffer.clear();
		return Factory.wrap(buffer);

	}

	/**
	 * Create a span buffer from a file. Uses the default buffer size.
	 * 
	 * @param file The file to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(File file) throws IOException {
		return wrap(file, MAX_MEM_BUFFER);
	}

	/**
	 * Create a span buffer from a file name. Uses the default buffer size.
	 * 
	 * @param fileName The file name to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName) throws IOException {
		return wrapFile(fileName, MAX_MEM_BUFFER);
	}

	/**
	 * Create a span buffer from a file.
	 * 
	 * @param file       the file to read.
	 * @param bufferSize the buffer size ot use.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(File file, long bufferSize) throws IOException {
		List<SpanBuffer> lazyLoader = getLoaders(new RandomAccessFile(file, "r"), bufferSize);
		return new SpanBufferList(0, lazyLoader);

	}

	/**
	 * Create a span buffer from a file name.
	 * 
	 * @param fileName   the file to read.
	 * @param bufferSize the buffer size ot use.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName, long bufferSize) throws IOException {
		List<SpanBuffer> lazyLoader = getLoaders(new RandomAccessFile(fileName, "r"), bufferSize);
		return new SpanBufferList(0, lazyLoader);
	}

	private static List<SpanBuffer> getLoaders(RandomAccessFile file, long bufferSize) throws IOException {
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = file.length();
		while (offset < limit) {
			buffers.add(new LazyLoadedBuffer(offset, new FileLazyLoader(file, offset, bufferSize)));
			offset += bufferSize;
		}
		return buffers;
	}

	/**
	 * Create a span buffer using a memory mapped file from a file.
	 * 
	 * @param file the file to read.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer asMemMap(File file) throws IOException {

		// create a memory mapped file
		RandomAccessFile aFile = null;
		try {
			aFile = new RandomAccessFile(file, "r");
			return wrap(aFile.getChannel());
		} finally {
			IOUtils.closeQuietly(aFile);
		}
	}

	/**
	 * Creates a span buffer from an input stream.
	 * 
	 * Copies the input stream to a SpanBufferOutputStream and then return the span
	 * buffer from that.
	 *
	 * @param inputStream the input stream to wrap.
	 * @return a SpanBuffer with an offset (start) of 0..
	 * @throws IOException on IOError
	 */
	public static SpanBuffer wrap(final InputStream inputStream) throws FileNotFoundException, IOException {
		if (inputStream instanceof SpanBufferInputStream) {
			return ((SpanBufferInputStream) inputStream).getSpanBuffer();
		}
		try (SpanBufferOutputStream sbos = new SpanBufferOutputStream()) {
			IOUtils.copyLarge(inputStream, sbos);
			sbos.flush();
			return sbos.getSpanBuffer();
		}
	}

	/**
	 * Wrap a CharSequence and set the offset to 0 (zero). Makes a string from the
	 * CharSequence and uses the bytes from that string as the buffer.
	 *
	 * @param sequence the CharSequence to wrap.
	 * @return A span buffer wrapping the CharSequence.
	 */
	public static SpanBuffer wrap(final CharSequence sequence) {
		return new SpanByteBuffer(ByteBuffer.wrap(sequence.toString().getBytes(StandardCharsets.UTF_8)));
	}

	/**
	 * Wrap a CharSequence and set the offset. Makes a string from the CharSequence
	 * and uses the bytes from that string as the buffer.
	 *
	 * @param offset   The offset that the SpanBuffer starts at.
	 * @param sequence the CharSequence to wrap.
	 * @return A span buffer wrapping the CharSequence.
	 */
	public static SpanBuffer wrap(final long offset, final CharSequence sequence) {
		return new SpanByteBuffer(offset, ByteBuffer.wrap(sequence.toString().getBytes(StandardCharsets.UTF_8)));
	}

	/**
	 * Wrap a byte array and set the offset to 0 (zero). Any change to the
	 * underlying byte buffer will be reflected in the SpanBuffer. This is not
	 * recommended.
	 *
	 * @param bytes the byte array to wrap.
	 * @return A span buffer wrapping the byte array.
	 */
	public static SpanBuffer wrap(final byte[] bytes) {
		return bytes.length == 0 ? Factory.EMPTY : new SpanByteBuffer(ByteBuffer.wrap(bytes));
	}

	/**
	 * Wrap a CharSequence and set the offset. Any change to the underlying byte
	 * buffer will be reflected in the SpanBuffer. This is not recommended.
	 *
	 * @param offset The offset that the SpanBuffer starts at.
	 * @param bytes  the byte array to wrap.
	 * @return A span buffer wrapping the byte array.
	 */
	public static SpanBuffer wrap(final long offset, final byte[] bytes) {
		return bytes.length == 0 ? (offset == 0 ? Factory.EMPTY : Factory.EMPTY.duplicate(offset))
				: new SpanByteBuffer(offset, ByteBuffer.wrap(bytes));
	}

	/**
	 * Wrap a byte array and set the offset to 0 (zero). Any change to the
	 * underlying byte buffer will be reflected in the SpanBuffer. This is not
	 * recommended.
	 *
	 * @param bytes  the byte array to wrap.
	 * @param off    The offset in the byte array to start the buffer at
	 * @param length the number of bytes the buffer should wrap.
	 * @return A span buffer wrapping the byte array.
	 */
	public static SpanBuffer wrap(final byte[] bytes, final int off, final int length) {
		return bytes.length == 0 ? Factory.EMPTY : new SpanByteBuffer(ByteBuffer.wrap(bytes, off, length));
	}

	/**
	 * Wrap a byte array and set the offset. Any change to the underlying byte
	 * buffer will be reflected in the SpanBuffer. This is not recommended.
	 *
	 * @param offset the offset (start) for the resulting SpanBuffer.
	 * @param bytes  the byte array to wrap.
	 * @param off    The offset in the byte array to start the buffer at
	 * @param length the number of bytes the buffer should wrap.
	 * @return A span buffer wrapping the byte array.
	 */
	public static SpanBuffer wrap(final long offset, final byte[] bytes, final int off, final int length) {
		return bytes.length == 0 ? (offset == 0 ? Factory.EMPTY : Factory.EMPTY.duplicate(offset))
				: new SpanByteBuffer(ByteBuffer.wrap(bytes, off, length));
	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer with an offset of 0 (zero). The
	 * buffers are concatenated and the total is used for the result.
	 *
	 * @param buffers The buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final SpanBuffer... buffers) {
		return Factory.merge(0, buffers);
	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer and set the offset. The buffers
	 * are concatenated and the total is used for the result.
	 *
	 * @param offset  the offset (start) of the resulting buffer.
	 * @param buffers The buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final long offset, final SpanBuffer... buffers) {
		return Factory.merge(offset, Arrays.asList(buffers).iterator());
	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer with an offset of 0 (zero). The
	 * buffers are concatenated and the total is used for the result.
	 *
	 * @param buffers An iterator on the buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final Iterator<SpanBuffer> buffers) {
		return Factory.merge(0, buffers);
	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer and set the offset. The buffers
	 * are concatenated and the total is used for the result.
	 *
	 * @param offset  the offset (start) of the resulting buffer.
	 * @param buffers An iterator on the buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final long offset, final Iterator<SpanBuffer> buffers) {

		final List<SpanBuffer> lst = new ArrayList<>();

		while (buffers.hasNext()) {
			final SpanBuffer buf = buffers.next();
			if (buf.getLength() > 0) {
				lst.add(buf);
			}
		}

		if (lst.isEmpty()) {
			if (0 == offset) {
				return Factory.EMPTY;
			}
			return Factory.EMPTY.duplicate(offset);
		}
		if (lst.size() == 1) {
			if (lst.get(0).getOffset() == offset) {
				return lst.get(0);
			}
			return lst.get(0).duplicate(offset);
		}
		return new SpanBufferList(offset, lst);

	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer with an offset of 0 (zero). The
	 * buffers are concatenated and the total is used for the result.
	 *
	 * @param buffers An iterator on the buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final Stream<SpanBuffer> buffers) {
		return Factory.merge(0, buffers.iterator());
	}

	/**
	 * Merge multiple SpanBuffers into a SpanBuffer and set the offset. The buffers
	 * are concatenated and the total is used for the result.
	 *
	 * @param offset  the offset (start) of the resulting buffer.
	 * @param buffers An iterator on the buffers to concatenate.
	 * @return A SpanBuffer comprising all the SpanBuffers provided.
	 */
	public static SpanBuffer merge(final long offset, final Stream<SpanBuffer> buffers) {
		return Factory.merge(offset, buffers.iterator());
	}

	/**
	 * Lazy loader for a random access file.
	 */
	public static class FileLazyLoader extends AbstractLazyLoader {

		private final RandomAccessFile file;
		private final long offset;

		/**
		 * Constructor
		 * 
		 * @param file       the random access file to read.
		 * @param offset     the offset of the file to read.
		 * @param bufferSize the default buffersize to use.
		 * @throws IOException on read error.
		 */
		public FileLazyLoader(RandomAccessFile file, long offset, long bufferSize) throws IOException {
			super((offset + bufferSize > file.length()) ? (file.length() - offset) : bufferSize);
			this.file = file;
			this.offset = offset;
		}

		@Override
		protected byte[] getBufferInternal() {
			byte[] buff = new byte[NumberUtils.checkIntLimit("buffer length", getLength())];
			try {
				synchronized (file) {
					file.seek(offset);
					file.read(buff);
				}
				return buff;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}
}