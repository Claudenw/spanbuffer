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
package org.xenei.spanbuffer.lazy.linear;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.xenei.span.LongSpan;
import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.lazy.LazyLoader;

/**
 * Lazy loader that utilizes off heap space.
 */
public class OffHeapLazyLoader implements LazyLoader {

	private final LazyLoader.Marker<FileChannel> marker;
	private final LongSpan span;
	private SoftReference<SpanBuffer> loadedBufferReference = null;

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param channel       the random access file to read.
	 * @param bufferSize    the buffer size.
	 * @param closeAfterUse if true the channel will be closed when it is no longer
	 *                      needed.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(FileChannel channel, long bufferSize, boolean closeAfterUse) throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(channel);
		if (closeAfterUse) {
			Factory.closableTracker.track(channel, marker);
		}
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = channel.size();
		while (offset < limit) {
			long len = Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new OffHeapLazyLoader(marker, offset, len)));
			offset += bufferSize;
		}
		return Factory.merge(buffers.iterator());

	}

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param randomAccessFile the random access file to read.
	 * @param bufferSize       the buffer size.
	 * @param closeAfterUse    if true the channel will be closed when it is no
	 *                         longer needed.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(RandomAccessFile randomAccessFile, long bufferSize, boolean closeAfterUse)
			throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(randomAccessFile.getChannel());
		if (closeAfterUse) {
			Factory.closableTracker.track(randomAccessFile, marker);
		}

		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = randomAccessFile.length();
		while (offset < limit) {
			long len = Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new OffHeapLazyLoader(marker, offset, len)));
			offset += bufferSize;
		}
		return Factory.merge(buffers.iterator());

	}

	/**
	 * Constructor
	 * 
	 * @param file       the random access file to read.
	 * @param offset     the offset of the file to read.
	 * @param bufferSize the default buffersize to use.
	 * @throws IOException on read error.
	 */
	public OffHeapLazyLoader(LazyLoader.Marker<FileChannel> marker, long offset, long length) throws IOException {
		this.marker = marker;
		this.span = (offset + length > marker.get().size()) ? LongSpan.fromEnd(offset, marker.get().size())
				: LongSpan.fromLength(offset, length);
	}

	@Override
	public synchronized SpanBuffer getBuffer() throws IOException {

		if ((loadedBufferReference == null) || (loadedBufferReference.get() == null)) {
			loadedBufferReference = new SoftReference<>(getBufferInternal());
		}
		return loadedBufferReference.get();
	}

	protected SpanBuffer getBufferInternal() throws IOException {
		NumberUtils.checkIntLimit("buffer length", getLength());
		MappedByteBuffer buff = marker.get().map(FileChannel.MapMode.READ_ONLY, span.getOffset(), getLength());
		return Factory.wrap(buff);
	}

	@Override
	public long getLength() {
		return span.getLength();
	}

}