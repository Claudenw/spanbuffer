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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;

public class TestSerializer implements TreeSerializer<TestPosition> {
	
	List<byte[]> buffers = new ArrayList<byte[]>();
	int maxBufferSize;
	
	public TestSerializer() {
		this( 10 );
	}
	
	public TestSerializer( int maxBufferSize )
	{
		this.maxBufferSize = maxBufferSize;
	}
	
	@Override
	public int getMaxBufferSize() {
		return 10;
	}

	@Override
	public TestPosition serialize(byte[] buffer) {
		buffers.add( buffer );
		return new TestPosition( buffers.size() - 1 );
	}

	@Override
	public ByteBuffer serialize( TestPosition position) {
		ByteBuffer bb =  ByteBuffer.allocate( Integer.BYTES );
		bb.putInt( ((TestPosition)position).idx);
		return bb;
	}

	@Override
	public TestPosition getNoDataPosition() {
		return TestPosition.NO_DATA;
	}
}
