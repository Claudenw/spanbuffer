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

import org.xenei.spanbuffer.lazy.tree.TreeLazyLoader;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;

/**
 * A convenience interface that combines serializer, deserializer and buffer
 * factory in one place.
 *
 * @param <P> the Position type.
 */
public abstract class AbstractSerde<P extends Position> {
	/**
	 * get the deserializer for this serde.
	 * 
	 * @return the deserializer.
	 */
	public abstract TreeDeserializer<P> getDeserializer();

	/**
	 * Get the serializer for this serde.
	 * 
	 * @return the serializer
	 */
	public abstract TreeSerializer<P> getSerializer();

	/**
	 * get the buffer factory for this serde.
	 * 
	 * @return the buffer factory.
	 */
	public abstract BufferFactory getFactory();
	
	/**
	 * Verify that the serde is valid.
	 * 
	 * To be valid
	 * <ul>
	 * <li>the factory must produce buffers that are 1 + (2*positionSize) long.
	 * Position size is specified by the serializer.</li>
	 * <li>the deserializer header size must be equal to the factory header size.</li>
	 * </ul>
	 * 
	 * @throwsIllegalArgumentException if the condition is not met.
	 */
	public void verify() {
		int minInnerLeafSize = 1 + (getSerializer().getPositionSize() * 2);
		if (getFactory().bufferSize() < minInnerLeafSize) {
			throw new IllegalArgumentException(
					String.format("Factory must produce buffers that are at least %s bytes long", minInnerLeafSize));
		}
		if (getFactory().headerSize() != getDeserializer().headerSize()) {
			throw new IllegalArgumentException(String.format(
					"Factory and Deserializer must specify the same header size. Factory=%s, Deserializer=%s",
					getFactory().headerSize(), getDeserializer().headerSize()));
		}
	}
	
	public TreeLazyLoader<P> getLazyLoader( P pos ) {
		return new TreeLazyLoader<P>(pos, getDeserializer());
	}
}
