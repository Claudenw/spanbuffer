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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.xenei.spanbuffer.SpanBuffer;

/**
 * Load a single buffer segment if needed.
 */
public interface LazyLoader {

	/**
	 * return the buffer if it has been loaded already, otherwise load and return.
	 * 
	 * @param inset the insert into the memory mapped buffer.
	 * @return fully loaded buffer
	 * @throws IOException on error
	 */
	SpanBuffer getBuffer(int inset) throws IOException;

	/**
	 * return the buffer length, load the buffer if unknown.
	 *
	 * @return length of buffer
	 */
	long getLength();

	/**
	 * A marker to determine when a closable object can be closed.
	 * 
	 * @param <T> A Closable type
	 */
	public class Marker<T extends Closeable> {
		private T closable;

		public Marker(T closable) {
			this.closable = closable;
		}

		public T get() {
			return closable;
		}
	}

}
