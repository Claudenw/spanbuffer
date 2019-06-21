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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.SpanBuffer.Walker;
import org.xenei.spanbuffer.lazy.tree.serde.Position;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;

public class TestDeserializer implements TreeDeserializer<TestPosition> {
	
	
	private List<byte[]> buffers;
	
	private Function<SpanBuffer, List<TreeLazyLoader<TestPosition,TestDeserializer>>> func;	
	
	public TestDeserializer(List<byte[]> buffers)
	{
		this.buffers= buffers;
		this.func = new Function<SpanBuffer, List<TreeLazyLoader<TestPosition,TestDeserializer>>>() {

				@Override
				public List<TreeLazyLoader<TestPosition,TestDeserializer>> apply(SpanBuffer buffer) {
					List<TreeLazyLoader<TestPosition,TestDeserializer>> result = new ArrayList<TreeLazyLoader<TestPosition,TestDeserializer>>();
					try (ObjectInputStream ois = new ObjectInputStream( buffer.getInputStream() )) {
						while (true) {
							try {
								int idx = ois.readInt();
								TreeLazyLoader<TestPosition,TestDeserializer> tll = new TreeLazyLoader<TestPosition,TestDeserializer>( new TestPosition( idx ), TestDeserializer.this);
								result.add( tll );
							} catch (EOFException e)
							{
								return result;
							}
						}
					} catch (IOException e)
					{
						throw new RuntimeException( e );
					}
					
				}};
				

	}
	
	@Override
	public byte[] deserialize(TestPosition position) {
		return position.isNoData()? new byte[0] : buffers.get(position.idx);
	}

	@Override
	public List<TreeLazyLoader<TestPosition, TestDeserializer>> extractLoaders( SpanBuffer buffer ) {
		List<TreeLazyLoader<TestPosition,TestDeserializer>> result = new ArrayList<TreeLazyLoader<TestPosition,TestDeserializer>>();
		try (DataInputStream ois = new DataInputStream( buffer.getInputStream() )) {
			while (true) {
				try {
					int idx = ois.readInt();
					TreeLazyLoader<TestPosition,TestDeserializer> tll = new TreeLazyLoader<TestPosition,TestDeserializer>( new TestPosition( idx ), TestDeserializer.this);
					result.add( tll );
				} catch (EOFException e)
				{
					return result;
				}
			}
		} catch (IOException e)
		{
			throw new RuntimeException( e );
		}
	}

	
}
