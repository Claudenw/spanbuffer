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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.TestSerde.TestDeserializer;
import org.xenei.spanbuffer.lazy.tree.node.HeapBufferFactory;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;
import org.xenei.spanbuffer.lazy.tree.serde.AbstractSerde;

@RunWith(Parameterized.class)
public class TreeRoundTripTest {

	@Parameters(name = "{0}")
	public static Collection<Object[]> parameters() {
		TestSerde ts = new TestSerde(new HeapBufferFactory(10));
		TestSerde bts = new TestSerde(new TestHeaderBufferFactory(10));
		return Arrays.asList(new Object[][] { { "No Offset", ts }, { "Offset", bts } });
	}

	private AbstractSerde<TestPosition> serde;

	/**
	 * Constructor
	 * 
	 * @param serde the Serde.
	 */

	public TreeRoundTripTest(String name, AbstractSerde<TestPosition> serde) {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
		this.serde = serde;
	}

	@Test
	public void testLong() throws IOException {
		TreeOutputStream tos = new TreeOutputStream(serde);
		String text = "Now is the time for all good men to come to the aid of their country";
		tos.write(text.getBytes());
		tos.close();
		TestPosition pos = (TestPosition) tos.getPosition();

		TreeLazyLoader<TestPosition> tll = serde.getLazyLoader(pos);

		SpanBuffer treeBuffer = tll.getBuffer(0);

		assertEquals(text, treeBuffer.getText());
	}

	@Test
	public void testShort() throws IOException {
		TreeOutputStream tos = new TreeOutputStream(serde);
		String text = "Now";
		tos.write(text.getBytes());
		tos.close();
		TestPosition pos = (TestPosition) tos.getPosition();
		List<ByteBuffer> lst = ((TestDeserializer) serde.getDeserializer()).buffers;

		assertEquals(0, pos.idx);
		assertEquals(InnerNode.OUTER_NODE_FLAG, lst.get(0).get(serde.getFactory().headerSize() + InnerNode.FLAG_BYTE));

		TreeLazyLoader<TestPosition> tll = serde.getLazyLoader(pos);

		SpanBuffer treeBuffer = tll.getBuffer(0);

		assertEquals(text, treeBuffer.getText());
	}

	@Test
	public void testEmpty() throws IOException {
		TreeOutputStream tos = new TreeOutputStream(serde);
		String text = "";
		tos.write(text.getBytes());
		tos.close();
		TestPosition pos = tos.getPosition();

		assertEquals(TestPosition.NO_DATA, pos);

		TreeLazyLoader<TestPosition> tll = serde.getLazyLoader(pos);

		SpanBuffer treeBuffer = tll.getBuffer(0);

		assertEquals(text, treeBuffer.getText());

	}
}
