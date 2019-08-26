package org.xenei.spanbuffer.lazy.linear;

import java.io.IOException;
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
 * Lazy loader for a random access file.
 */
public class FileChannelLazyLoader implements LazyLoader {

	private final LazyLoader.Marker<FileChannel> marker;
	private final LongSpan span;
	private SoftReference<SpanBuffer> loadedBufferReference = null;
	

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * Uses default internal buffer size
	 * 
	 * @see LazyLoadedBuffer#DEFAULT_INTERNAL_BUFFER_SIZE
	 * 
	 * @param channel    the random access file to read.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(FileChannel channel) throws IOException {
		return load( channel, LazyLoadedBuffer.DEFAULT_INTERNAL_BUFFER_SIZE);
	}
	
	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param channel    the random access file to read.
	 * @param bufferSize the buffer size.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(FileChannel channel, long bufferSize) throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(channel);
		LazyLoader.tracker.track(channel, marker);
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = channel.size();
		while (offset < limit) {
			long len = Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new FileChannelLazyLoader(marker, offset, len)));
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
	public FileChannelLazyLoader(LazyLoader.Marker<FileChannel> marker, long offset, long length) throws IOException {
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
		MappedByteBuffer buff = this.marker.get().map(FileChannel.MapMode.READ_ONLY, span.getOffset(), getLength());
		return Factory.wrap(buff);
	}

	@Override
	public long getLength() {
		return span.getLength();
	}

}