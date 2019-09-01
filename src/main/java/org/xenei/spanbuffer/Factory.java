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

import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.impl.SpanBufferList;
import org.xenei.spanbuffer.impl.SpanByteBuffer;
import org.xenei.spanbuffer.lazy.ClosableCleaningTracker;
import org.xenei.spanbuffer.lazy.linear.OffHeapLazyLoader;
import org.xenei.spanbuffer.lazy.linear.OnHeapLazyLoader;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

/**
 * A factory to create spanbuffers and to merge them
 *
 */
public final class Factory {

	/**
	 * The default maximum heap size allowed for a single buffer. If the buffer size
	 * exceeds this then off heap memory is used. This is the limit used when
	 * loading files or data streams.
	 */
	/* package private for testing */
	/* package private */ static int MAX_HEAP = NumberUtils.checkIntLimit("MAX_HEAP", 32 * FileUtils.ONE_MB);

	/**
	 * The default length for the internal buffer of a lazy loaded buffer.
	 */
	public static final int DEFAULT_INTERNAL_BUFFER_SIZE = (int)(4 * FileUtils.ONE_MB);

	/**
	 * The tracker that manages closing objects when final spanbuffer is garbage
	 * collected.
	 * 
	 * Some lazy loaders use objects that should be closed when the last spanbuffer
	 * using the object is garbage collected. This class ensures that that close
	 * occurs.
	 */
	public static final ClosableCleaningTracker closableTracker = new ClosableCleaningTracker();
	/**
	 * The tracker that manages removing files when the final spanbuffer is garbage
	 * collected.
	 * 
	 * Some lazy loaders use files that should be deleted when the last spanbuffer
	 * using the object is garbage collected. This class ensures that that delete
	 * occurs.
	 * 
	 * @See http://commons.apache.org/proper/commons-io/javadocs/api-release/index.html?org/apache/commons/io/FileCleaningTracker.html
	 */
	public static final FileCleaningTracker fileTracker = new FileCleaningTracker();

	/**
	 * Sets the maximum heap size used when creating buffers from files or input
	 * streams. If the total size is greater than max heap a memory mapped file
	 * buffer is used otherwise the data is read into a standard memory buffer.
	 * Initial default value is 32MB.
	 *
	 * @param newLimit The new memory limit
	 * @return the old memory limit
	 */
	public static int setMaxHeap(final int newLimit) {
		final int retval = Factory.MAX_HEAP;
		Factory.MAX_HEAP = newLimit;
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
	public static long getMaxHeap() {
		return Factory.MAX_HEAP;
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
	 * Create a span buffer from a random access file. Uses default internal buffer
	 * size
	 * <p>
	 * does not close the random access file.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader.
	 * </p>
	 * 
	 * @param randomAccessFile The random access file to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final RandomAccessFile randomAccessFile) throws FileNotFoundException, IOException {
		return wrap(randomAccessFile, DEFAULT_INTERNAL_BUFFER_SIZE, false);

	}

	/**
	 * Create a span buffer from a random access file.
	 * 
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader.
	 * </p>
	 * 
	 * @param randomAccessFile The random access file to read
	 * @param closeAfterUse    if true file will be closed when no longer needed.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final RandomAccessFile randomAccessFile, boolean closeAfterUse)
			throws FileNotFoundException, IOException {
		return wrap(randomAccessFile, DEFAULT_INTERNAL_BUFFER_SIZE, closeAfterUse);

	}

	/**
	 * Create a span buffer from a random access file.
	 * <p>
	 * Does not close the random access file.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader.
	 * </p>
	 * 
	 * @param randomAccessFile The random access file to read
	 * @param bufferSize       the size of the internal buffer.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final RandomAccessFile randomAccessFile, int bufferSize)
			throws FileNotFoundException, IOException {
		return wrap(randomAccessFile, bufferSize, false);
	}

	/**
	 * Create a span buffer from a random access file.
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader.
	 * </p>
	 * 
	 * @param randomAccessFile The random access file to read
	 * @param bufferSize       the size of the internal buffer.
	 * @param closeAfterUse    if true file will be closed when no longer needed.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final RandomAccessFile randomAccessFile, int bufferSize, boolean closeAfterUse)
			throws FileNotFoundException, IOException {
		if (randomAccessFile == null) {
			throw new IllegalArgumentException("randomAccessFile must not be a null");
		}

		// small enough for on heap use
		if (randomAccessFile.length() <= MAX_HEAP) {
			if (randomAccessFile.length() <= bufferSize) {
				byte[] buffer = new byte[(int) randomAccessFile.length()];
				randomAccessFile.readFully(buffer);
				if (closeAfterUse) {
					randomAccessFile.close();
				}
				return wrap(buffer);
			} else {
				// use on heap lazy loader
				return OnHeapLazyLoader.load(randomAccessFile, bufferSize, true);
			}
		} else {
			// use off heap lazy loader
			return OffHeapLazyLoader.load(randomAccessFile, bufferSize, true);
		}

	}

	/**
	 * Create a span buffer from a FileChannel. Uses default internal buffer size
	 * <p>
	 * Does not close the channel after use.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader.
	 * </p>
	 * 
	 * @param fileChannel The file channel to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final FileChannel fileChannel) throws FileNotFoundException, IOException {
		if (fileChannel == null) {
			throw new IllegalArgumentException("FileChannel must not be a null");
		}
		return wrap(fileChannel, DEFAULT_INTERNAL_BUFFER_SIZE, false);
	}

	/**
	 * Create a span buffer from a FileChannel.
	 * <p>
	 * Does not close the channel after use.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader.
	 * </p>
	 *
	 * @param fileChannel The file channel to read
	 * @param bufferSize  the size of the internal buffer.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final FileChannel fileChannel, int bufferSize)
			throws FileNotFoundException, IOException {
		if (fileChannel == null) {
			throw new IllegalArgumentException("FileChannel must not be a null");
		}
		return wrap(fileChannel, bufferSize, false);
	}

	/**
	 * Create a span buffer from a FileChannel.
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader.
	 * </p>
	 * 
	 * @param fileChannel    The file channel to read
	 * @param bufferSize     the size of the internal buffer.
	 * @param closeAfterUser if true the channel will be closed when it is no longer
	 *                       needed.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(final FileChannel fileChannel, int bufferSize, boolean closeAfterUse)
			throws FileNotFoundException, IOException {
		if (fileChannel == null) {
			throw new IllegalArgumentException("FileChannel must not be a null");
		}

		// small enough for on heap use
		if (fileChannel.size() <= MAX_HEAP) {
			if (fileChannel.size() <= bufferSize) {
				MappedByteBuffer buff = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
				if (closeAfterUse) {
					closableTracker.track(fileChannel, buff);
				}
				return wrap(buff);
			} else {
				// use on heap lazy loader
				return OnHeapLazyLoader.load(fileChannel, bufferSize, closeAfterUse);
			}
		} else {
			// use off heap lazy loader
			return OffHeapLazyLoader.load(fileChannel, bufferSize, closeAfterUse);
		}
	}

	/**
	 * Create a span buffer from a file. Uses the default buffer size.
	 * <p>
	 * Does not delete the file after use.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader.
	 * </p>
	 * 
	 * @param file The file to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(File file) throws IOException {
		return wrap(file, DEFAULT_INTERNAL_BUFFER_SIZE, false);
	}

	/**
	 * Create a span buffer from a file name. Uses the default buffer size.
	 * <p>
	 * Does not delete the file after use.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader.
	 * </p>
	 * 
	 * @param fileName The file name to read
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName) throws IOException {
		return wrap(new File(fileName), DEFAULT_INTERNAL_BUFFER_SIZE, false);
	}

	/**
	 * Create a span buffer from a file name. Uses the default buffer size.
	 * 
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader..
	 * </p>
	 * 
	 * @param fileName       The file name to read
	 * @param deleteAfterUse if true file will be deleted when no longer needed.
	 * @return A SpanBuffer with an offset (start) of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName, boolean deleteAfterUse) throws IOException {
		return wrap(new File(fileName), DEFAULT_INTERNAL_BUFFER_SIZE, deleteAfterUse);
	}

	/**
	 * Create a span buffer from a file.
	 * <p>
	 * Does not delete the file.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader..
	 * </p>
	 * 
	 * @param file       the file to read.
	 * @param bufferSize the buffer size to use.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(File file, int bufferSize) throws IOException {
		return wrap(file, bufferSize, false);
	}

	/**
	 * Create a span buffer from a file.
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of DEFAULT_INTERNAL_BUFFER_SIZE
	 * will be used by a lazy loader..
	 * </p>
	 * 
	 * @param file           the file to read.
	 * @param deleteAfterUse if true file will be deleted when no longer needed.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrap(File file, boolean deleteAfterUse) throws IOException {
		return wrap(file, DEFAULT_INTERNAL_BUFFER_SIZE, deleteAfterUse);
	}

	/**
	 * Create a span buffer from a file.
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader..
	 * </p>
	 * 
	 * @param file           the file to read.
	 * @param bufferSize     the buffer size ot use.
	 * @param deleteAfterUse if true file will be deleted when no longer needed.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException on IO error
	 */
	public static SpanBuffer wrap(File file, int bufferSize, boolean deleteAfterUse) throws IOException {
		RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
		if (deleteAfterUse) {
			fileTracker.track(file, randomAccessFile);
		}
		return wrap(randomAccessFile, bufferSize, true);
	}

	/**
	 * Create a span buffer from a file name.
	 * <p>
	 * Does not delete the file after use.
	 * </p>
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader..
	 * </p>
	 * 
	 * @param fileName       the file to read.
	 * @param bufferSize     the buffer size ot use.
	 * @param deleteAfterUse if true file will be deleted when no longer needed.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName, int bufferSize) throws IOException {
		return wrap(new File(fileName), bufferSize, false);
	}

	/**
	 * Create a span buffer from a file name.
	 * <p>
	 * If the file is smaller than MAX_HEAP the entire file is read into heap. If
	 * the file is larger than MAX_HEAP then buffers of bufferSize will be used by a
	 * lazy loader..
	 * </p>
	 * 
	 * @param fileName   the file to read.
	 * @param bufferSize the buffer size to use.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	public static SpanBuffer wrapFile(String fileName, int bufferSize, boolean deleteAfterUse) throws IOException {
		return wrap(new File(fileName), bufferSize, deleteAfterUse);
	}

	/**
	 * Create a span buffer using a memory mapped file from a file.
	 * <p>
	 * Uses default internal buffer size
	 * </p>
	 * 
	 * @param file the file to read.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	@SuppressWarnings("resource")
	public static SpanBuffer asMemMap(File file) throws IOException {
		return asMemMap(file, DEFAULT_INTERNAL_BUFFER_SIZE);
	}

	/**
	 * Create a span buffer using a memory mapped file from a file.
	 * 
	 * @param file       the file to read.
	 * @param bufferSize the size of the internal buffer.
	 * @return a SpanBuffer with an offset of 0.
	 * @throws IOException           on IO error
	 * @throws FileNotFoundException on file not found.
	 */
	@SuppressWarnings("resource")
	public static SpanBuffer asMemMap(File file, int bufferSize) throws IOException {
		return wrap(new RandomAccessFile(file, "r").getChannel(), bufferSize, true);
	}

	/**
	 * Creates a span buffer from an input stream.
	 * <p>
	 * Does not close the input stream
	 * </p>
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
				: new SpanByteBuffer(offset, ByteBuffer.wrap(bytes, off, length));
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

}