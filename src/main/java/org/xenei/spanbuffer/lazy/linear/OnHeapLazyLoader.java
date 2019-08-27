package org.xenei.spanbuffer.lazy.linear;

import java.io.IOException;
import java.io.RandomAccessFile;
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
	public static SpanBuffer load(RandomAccessFile file, long bufferSize, boolean closeAfterUse) throws IOException {
		LazyLoader.Marker<FileChannel> marker = new LazyLoader.Marker<FileChannel>(file.getChannel());
		if (closeAfterUse) {
			Factory.closableTracker.track(file, marker);
		}
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = file.length();
		while (offset < limit) {
			long len = Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new OnHeapLazyLoader(marker, offset, len)));
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
	private OnHeapLazyLoader(LazyLoader.Marker<FileChannel> marker, long offset, long bufferSize) throws IOException {
		super((offset + bufferSize > marker.get().size()) ? (marker.get().size() - offset) : bufferSize);
		this.marker = marker;
		this.offset = offset;
	}

	@Override
	protected SpanBuffer getBufferInternal() throws IOException {
		return Factory.wrap(marker.get().map(MapMode.READ_ONLY, offset, getLength()));

	}

}