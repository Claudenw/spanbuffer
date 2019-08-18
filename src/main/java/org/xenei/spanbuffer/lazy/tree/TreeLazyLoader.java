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
package org.xenei.spanbuffer.lazy.tree;

import java.util.List;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.AbstractLazyLoader;
import org.xenei.spanbuffer.lazy.tree.serde.Position;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;

/**
 * Class that manages the loading of tree data.
 * 
 * @param <P> the Position implementation.
 * @param <T> The TreeDeserializer
 *            <P>
 *            implementation.
 */
public class TreeLazyLoader<P extends Position, T extends TreeDeserializer<P>> extends AbstractLazyLoader {

	private T deserializer;
	private P position;

	/**
	 * Constructor.
	 * 
	 * @param position     The position in the deserializer to read the data from.
	 * @param deserializer the deserializer to read from.
	 */
	public TreeLazyLoader(P position, T deserializer) {
		this.deserializer = deserializer;
		this.position = position;
	}

	/**
	 * Return this lazy loader as a span buffer.
	 * 
	 * @return this lazy loader as a span buffer.
	 */
	public SpanBuffer asSpanBuffer() {
		if (position.isNoData()) {
			return Factory.EMPTY;
		}
		return new InnerBuffer(this);
	}

	/**
	 * Apply the deserializer map to the buffer to create a list of TreeLazyLoaders.
	 * 
	 * @return a list of TreeLazyLoader<P,T> from the buffer.
	 */
	public final List<TreeLazyLoader<P, T>> applyMap(SpanBuffer buffer) {
		return deserializer.extractLoaders(buffer);
	}

	/**
	 * Return true if the buffer has no data.
	 * 
	 * @return true if the buffer has no data.
	 */
	public final boolean hasNoData() {
		return position.isNoData();
	}

	@Override
	protected byte[] getBufferInternal() {
		return position.isNoData() ? new byte[0] : deserializer.deserialize(position);
	}
}
