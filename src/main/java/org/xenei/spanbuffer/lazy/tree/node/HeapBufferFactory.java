package org.xenei.spanbuffer.lazy.tree.node;

import java.nio.ByteBuffer;

public class HeapBufferFactory implements BufferFactory {
	private int bufferSize;
	
	public HeapBufferFactory(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	@Override
	public int bufferSize() {
		return bufferSize;
	}

	@Override
	public ByteBuffer createBuffer() {
		return ByteBuffer.allocate(bufferSize);
	}

	@Override
	public int headerSize() {
		return 0;
	}

	@Override
	public void free(ByteBuffer buffer) {
		// no action required
	}

}
