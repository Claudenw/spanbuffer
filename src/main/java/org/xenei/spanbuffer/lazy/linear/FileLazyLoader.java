package org.xenei.spanbuffer.lazy.linear;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.AbstractLazyLoader;
import org.xenei.spanbuffer.lazy.LazyLoadedBuffer;
import org.xenei.spanbuffer.lazy.LazyLoader;

/**
 * Lazy loader for a random access file.
 */
public class FileLazyLoader extends AbstractLazyLoader {

	private final LazyLoader.Marker<RandomAccessFile> marker; 
	private final long offset;

	/**
	 * Create a span buffer from a random access file and a buffer size. Uses
	 * default internal buffer size
	 * 
	 * @see LazyLoadedBuffer#DEFAULT_INTERNAL_BUFFER_SIZE
	 * 
	 * @param file       the random access file to read.
	 * @param bufferSize the buffer size.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(RandomAccessFile file) throws IOException {
		return load(file, LazyLoadedBuffer.DEFAULT_INTERNAL_BUFFER_SIZE);
	}

	/**
	 * Create a span buffer from a random access file and a buffer size.
	 * 
	 * @param file       the random access file to read.
	 * @param bufferSize the buffer size.
	 * @return A lazy loaded span buffer.
	 * @throws IOException on error.
	 */
	public static SpanBuffer load(RandomAccessFile file, long bufferSize) throws IOException {
		LazyLoader.Marker<RandomAccessFile> marker = new LazyLoader.Marker<RandomAccessFile>( file );
		LazyLoader.tracker.track(file, marker);
		List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
		long offset = 0;
		long limit = file.length();
		while (offset < limit) {
			long len = Long.min(limit - offset, bufferSize);
			buffers.add(new LazyLoadedBuffer(offset, new FileLazyLoader(marker, offset, len)));
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
	private FileLazyLoader(LazyLoader.Marker<RandomAccessFile> marker, long offset, long bufferSize) throws IOException {
		super((offset + bufferSize > marker.get().length()) ? (marker.get().length() - offset) : bufferSize);
		this.marker = marker;
		this.offset = offset;
	}

	@Override
	protected SpanBuffer getBufferInternal() throws IOException {
		byte[] buff = new byte[NumberUtils.checkIntLimit("buffer length", getLength())];

		synchronized (marker.get()) {
			marker.get().seek(offset);
			marker.get().read(buff);
		}
		return Factory.wrap(buff);

	}

}