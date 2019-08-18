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
package org.xenei.spanbuffer.lazy.tree.serde;

import java.nio.ByteBuffer;

/**
 * Serializer to write tree data to storage.
 * 
 * @param <P> The Position implementation used by this serializer.
 */
public interface TreeSerializer<P extends Position> {

	/**
	 * Get the maximum buffer that the serializer can handle in one buffer.
	 * 
	 * @return the maximum buffer size.
	 */
	public int getMaxBufferSize();

	/**
	 * Write the buffer to the data storage and return a Position.
	 * 
	 * @param buffer the buffer to write
	 * @return The Position the buffer was written to.
	 */
	public P serialize(byte[] buffer);

	/**
	 * Write the Position information to a byte buffer.
	 * 
	 * @param position the position to write to the buffer.
	 * @return a byte buffer containing the position information.
	 */
	public ByteBuffer serialize(P position);

	/**
	 * Return a position that means no data has been written.
	 * 
	 * @return the no data position for this serializer.
	 */
	public P getNoDataPosition();

}
