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
package org.xenei.spanbuffer.impl;

import java.nio.ByteBuffer;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.spanbuffer.SpanBufferContractTest.SpanBufferProducer;
import org.xenei.spanbuffer.impl.SpanByteBuffer;

@RunWith(ContractSuite.class)
@ContractImpl(SpanByteBuffer.class)
public class OffsetSpanByteBufferContractSuite {

	ByteBuffer bb;

	public OffsetSpanByteBufferContractSuite() {
		bb = ByteBuffer.wrap("xxHello World".getBytes());
		bb.position(2);
	}

	@Contract.Inject
	public SpanBufferProducer<SpanByteBuffer> getProducer() {
		return new SpanBufferProducer<SpanByteBuffer>() {

			@Override
			public SpanByteBuffer newInstance() {
				return new SpanByteBuffer(bb);
			}

			@Override
			public void cleanUp() {
				// do nothing
			}

			@Override
			public byte[] getBufferContents() {
				return "Hello World".getBytes();
			}
		};
	}
}
