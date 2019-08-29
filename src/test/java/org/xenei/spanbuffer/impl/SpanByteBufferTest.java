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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.impl.SpanByteBuffer;

public class SpanByteBufferTest {

	@Test
	public void constructorTest() throws IOException {

		final SpanByteBuffer sp1 = new SpanByteBuffer(ByteBuffer.wrap("Hello World".getBytes()));
		Assert.assertEquals(0, sp1.getOffset());
		Assert.assertEquals(10, sp1.getEnd());
		Assert.assertEquals(11, sp1.getLength());

	}

	@Test
	public void constructorWithOFfsetTest() throws IOException {

		final SpanByteBuffer sp1 = new SpanByteBuffer(5, ByteBuffer.wrap("Hello World".getBytes()));
		Assert.assertEquals(5, sp1.getOffset());
		Assert.assertEquals(15, sp1.getEnd());
		Assert.assertEquals(11, sp1.getLength());

	}

	@Test
	public void testPositionSet() throws IOException {
		String text = "Hello World";
		ByteBuffer bb = ByteBuffer.wrap(text.getBytes());
		bb.position(6);
		SpanBuffer sb = new SpanByteBuffer(bb);
		Assert.assertEquals(5, sb.getLength());
		Assert.assertEquals(0, sb.getOffset());
		Assert.assertEquals(4, sb.getEnd());
		Assert.assertEquals("World", sb.getText());
	}

	@Test
	public void testLimitSet() throws IOException {
		String text = "Hello World  ";
		ByteBuffer bb = ByteBuffer.wrap(text.getBytes());
		bb.limit("Hello World".length());
		SpanBuffer sb = new SpanByteBuffer(bb);
		Assert.assertEquals("Hello World".length(), sb.getLength());
		Assert.assertEquals(0, sb.getOffset());
		Assert.assertEquals("Hello World".length() - 1, sb.getEnd());
		Assert.assertEquals("Hello World", sb.getText());
	}

	@Test
	public void testCut() throws IOException {
		String text = "Hello World";
		ByteBuffer bb = ByteBuffer.wrap(text.getBytes());
		bb.position(6);
		SpanBuffer sb = new SpanByteBuffer(bb);
		SpanBuffer sb2 = sb.cut(2);
		Assert.assertEquals("rld", sb2.getText());
		Assert.assertEquals(6, bb.position());
	}

}
