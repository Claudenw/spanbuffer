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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.AbstractLazyLoader;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.lazy.LazyLoader;

/**
 * Lazy loader that utilizes heap space.
 */
public class OnHeapLazyLoader extends AbstractLazyLoader {

	private final LazyLoader.Marker<FileChannel> marker;
	private final long offset;

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param file          the random access file to read.
	 * @param bufferSize    the buffer size.
	 * @param closeAfterUse if true the channel will be closed when it is no longer
	 *                      needed.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(RandomAccessFile file, int bufferSize, boolean closeAfterUse) throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(file.getChannel());
		if (closeAfterUse) {
			Factory.closableTracker.track(file, marker);
		}
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = file.length();
		while (offset < limit) {
			int len = (int) Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, 0, len, new OnHeapLazyLoader(marker, offset, len)));
			offset += bufferSize;
		}
		return Factory.merge(buffers.iterator());

	}

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param channel       the file channel to read.
	 * @param bufferSize    the buffer size.
	 * @param closeAfterUse if true the channel will be closed when it is no longer
	 *                      needed.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(FileChannel channel, int bufferSize, boolean closeAfterUse) throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(channel);
		if (closeAfterUse) {
			Factory.closableTracker.track(channel, marker);
		}
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = channel.size();
		while (offset < limit) {
			int len = (int)Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new OnHeapLazyLoader(marker, offset, len)));
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
	private OnHeapLazyLoader(LazyLoader.Marker<FileChannel> marker, long offset, int bufferSize) throws IOException {
		super((offset + bufferSize > marker.get().size()) ? (marker.get().size() - offset) : bufferSize);
		this.marker = marker;
		this.offset = offset;
	}

	@Override
	protected SpanBuffer getBufferInternal(int inset) throws IOException {
		// length was set in the constructor.
		ByteBuffer bb = ByteBuffer.allocate((int)getLength()-inset);
		marker.get().read( bb, offset+inset );
		bb.position(0);
		return Factory.wrap( offset+inset, bb );
	}

}