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
package org.xenei.spanbuffer.lazy;

import java.io.IOException;
import java.lang.ref.SoftReference;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * Abstract class for lazy loading. Keeps a soft reference to the loaded bytes
 * so the garbage collector can dispose of them when necessary.
 */
public abstract class AbstractLazyLoader implements LazyLoader {

	private SoftReference<SpanBuffer> loadedBufferReference = null;
	private Long length = null;

	/**
	 * Method to load the internal buffer.
	 * 
	 * @param the inset into the lazy loaded buffer
	 * @return the bytes for the internal buffer.
	 * @throws IOException on error
	 */
	protected abstract SpanBuffer getBufferInternal(int inset) throws IOException;

	/**
	 * allows for creation where the length isn't known upfront.
	 */
	public AbstractLazyLoader() {
	}

	/**
	 * Allows length to be specified up-front when known - so that don't need to
	 * invoke get() method to get it.
	 *
	 * @param length to be specified
	 */
	public AbstractLazyLoader(final Long length) {
		this();
		this.length = length;
	}

	@Override
	public synchronized SpanBuffer getBuffer(int inset) throws IOException {

		if ((loadedBufferReference == null) || (loadedBufferReference.get() == null)) {
			loadedBufferReference = new SoftReference<>(getBufferInternal(0));
		}
		return loadedBufferReference.get();
	}

	@Override
	public long getLength() {
		if (length == null) {
			try {
				length = getBuffer(0).getLength();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		return length;
	}

}
