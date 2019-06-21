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

import java.util.List;


import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.TreeLazyLoader;

/**
 * An interface that defines how to deserialize data from the storage layer.
 *
 * @param <P> The Position implementation used by this deserializer.
 */
public interface TreeDeserializer<P extends Position> {

	/**
	 * Read from the Position and return the associated byte buffer.
	 * 
	 * The position parameter was returned from an earlier call to TreeSerializer.serialize().
	 * 
	 * @param position the position to read the buffer from.
	 * @return a byte buffer containing data that was serialized at the position.
	 */
	public byte[] deserialize( P position );
	
	/**
	 * process a spanbuffer that contains one or more positions serialized by 
	 * TreeSerializer.serialize(position) and return a list of TreeLazyLoader instances.
	 * 
	 * @param buffer the buffer to process.
	 * @return A list of TreeLazyLoader instances.
	 */
	public <T extends TreeDeserializer<P>> List<TreeLazyLoader<P, T>> extractLoaders(SpanBuffer buffer );
}
