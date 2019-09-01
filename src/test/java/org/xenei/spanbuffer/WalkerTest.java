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
package org.xenei.spanbuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.xenei.spanbuffer.impl.SpanByteBuffer;

public class WalkerTest {
	byte b = (byte) 0x5;
	int i = Integer.MAX_VALUE / 2;
	long l = Long.MAX_VALUE / 2;
	boolean bl = true;
	char c = 'c';
	double d = Double.MAX_VALUE / 2;
	float f = Float.MAX_VALUE / 2;
	byte[] barry = "Hello World".getBytes();
	String line = "Hello again\n";
	short s = Short.MAX_VALUE / 2;
	byte ub = (byte) 0xFE;
	int us = Short.MAX_VALUE + 1;
	String utf = "This is the UTF String";
	int skipLen = 10;
	byte[] buff = new byte[skipLen];

	SpanBuffer sb;

	public WalkerTest() throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);) {
			dos.write(b);
			dos.writeInt(i);
			dos.writeLong(l);
			dos.writeBoolean(bl);
			dos.writeChar(c);
			dos.writeDouble(d);
			dos.writeFloat(f);
			dos.write(barry);
			dos.writeBytes(line);
			dos.writeShort(s);
			dos.writeByte(ub);
			dos.writeShort(us);
			dos.writeUTF(utf);
			dos.write(buff);
			dos.write(b);
			dos.flush();
			ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
			sb = new SpanByteBuffer(bb);
		}
	}

	/*
	 * hasCurrent is test in reads test.
	 */

	@Test
	public void testReads() throws IOException {
		Walker walker = sb.getWalker();
		assertTrue(walker.hasCurrent());
		assertEquals(b, walker.readByte());
		assertEquals(i, walker.readInt());
		assertEquals(l, walker.readLong());
		assertEquals(bl, walker.readBoolean());
		assertEquals(c, walker.readChar());
		assertEquals(d, walker.readDouble(), 0.0);
		assertEquals(f, walker.readFloat(), 0.0f);
		byte[] ba = new byte[barry.length];
		assertEquals(barry.length, walker.read(ba));
		assertArrayEquals(barry, ba);
		assertEquals("Hello again", walker.readLine());
		assertEquals(s, walker.readShort());
		assertEquals(0xFE, walker.readUnsignedByte());
		assertEquals(Short.MAX_VALUE + 1, walker.readUnsignedShort());
		assertEquals(utf, walker.readUTF());
		assertEquals(skipLen, walker.skipBytes(skipLen));
		assertEquals(b, walker.readByte());
		assertFalse("Should be at eof", walker.hasCurrent());
	}

	@Test
	public void testGetBuffer() {
		Walker walker = sb.getWalker();
		assertEquals(sb, walker.getBuffer());
	}

	@Test
	public void testGetByte() throws IOException {
		Walker walker = sb.getWalker();
		assertEquals(b, walker.getByte());
		assertEquals("getByte should not move walker", b, walker.getByte());

	}

	/*
	 * Also tests increment.
	 */
	@Test
	public void testGetPos() throws IOException {
		Walker walker = sb.getWalker();
		assertEquals(0, walker.getPos());
		walker.increment(10);
		assertEquals(10, walker.getPos());
		walker.increment(-5);
		assertEquals(5, walker.getPos());
		walker.setPosition(7);
		assertEquals(7, walker.getPos());
	}

	@Test
	public void testReadBytes() throws IOException {
		sb = Factory.wrap("Hello World");
		byte[] buff = new byte[5];

		Walker walker = sb.getWalker();
		assertEquals(0, walker.getPos());
		assertEquals(5, walker.read(buff));
		assertArrayEquals("Hello".getBytes(), buff);
		assertEquals(5, walker.getPos());
		assertEquals(1, walker.read(buff, 4, 1));
		assertArrayEquals("Hell ".getBytes(), buff);
		assertEquals(6, walker.getPos());
	}
	
	@Test
	public void testSpanBufferReads() throws IOException {
		sb = Factory.wrap("Hello World");
		ByteBuffer buff = ByteBuffer.allocate(5);

		Walker walker = sb.getWalker();
		assertEquals(0, walker.getPos());
		assertEquals(5, walker.read(buff));
		
		assertArrayEquals("Hello".getBytes(), buff.array());
		assertEquals(5, walker.getPos());
		buff.position(4);
		assertEquals(1, walker.read(buff));
		assertArrayEquals("Hell ".getBytes(), buff.array());
		assertEquals(6, walker.getPos());
	}

}
