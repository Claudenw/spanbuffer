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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.xpath.XPathResult;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;
import org.xenei.spanbuffer.lazy.tree.serde.SerdeImpl;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;

public class TestSerde extends SerdeImpl<TestPosition> {

	public TestSerde(BufferFactory factory) {
		this(factory, new TestSerializer(factory.bufferSize()));
	}

	private TestSerde(BufferFactory factory, TestSerializer ts) {
		super(factory, ts, new TestDeserializer(factory.headerSize(), ts.buffers));
	}

	public static class TestSerializer implements TreeSerializer<TestPosition> {

		List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
		int maxBufferSize;

		public TestSerializer() {
			this(10);
		}

		public TestSerializer(int maxBufferSize) {
			this.maxBufferSize = maxBufferSize;
		}

		@Override
		public int getMaxBufferSize() {
			return maxBufferSize;
		}

		@Override
		public TestPosition serialize(ByteBuffer buffer) {
			buffers.add(buffer);
			return new TestPosition(buffers.size() - 1);
		}

		@Override
		public ByteBuffer serialize(TestPosition position) {
			ByteBuffer result = ByteBuffer.allocate(Integer.BYTES).putInt(position.idx);
			result.flip();
			return result;
		}

		@Override
		public TestPosition getNoDataPosition() {
			return TestPosition.NO_DATA;
		}

		@Override
		public int getPositionSize() {
			return Integer.BYTES;
		}
	}

	public static class TestDeserializer implements TreeDeserializer<TestPosition> {

		/* package private for testing */
		List<ByteBuffer> buffers;
		private int headerSize;

		public TestDeserializer(int headerSize, List<ByteBuffer> buffers) {
			this.headerSize = headerSize;
			this.buffers = buffers;
		}

		@Override
		public int headerSize() {
			return headerSize;
		}

		@Override
		public ByteBuffer deserialize(TestPosition position) {
		    ByteBuffer result = null;
			if (!position.isNoData()) {
			    result = buffers.get(position.idx).duplicate();
			    result.flip();
			    result.position(headerSize);
			}
			return result;
		}

		@Override
		public List<TreeLazyLoader<TestPosition>> extractLoaders(SpanBuffer buffer) {
			List<TreeLazyLoader<TestPosition>> result = new ArrayList<TreeLazyLoader<TestPosition>>();
			try (DataInputStream ois = new DataInputStream(buffer.getInputStream())) {
				while (true) {
					try {
						int idx = ois.readInt();
						TreeLazyLoader<TestPosition> tll = new TreeLazyLoader<TestPosition>(new TestPosition(idx),
								this);
						result.add(tll);
					} catch (EOFException e) {
						return result;
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}

}
