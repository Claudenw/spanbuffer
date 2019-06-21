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

import org.junit.Test;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;

public class TreeRoundTripTest {
	
	
	
	@Test
	public void testLong() throws IOException {
		TestSerializer ts = new TestSerializer();
		TreeOutputStream tos = new TreeOutputStream( ts );
		String text = "Now is the time for all good men to come to the aid of their country"; 
		tos.write( text.getBytes());
		tos.close();
		TestPosition pos = (TestPosition) tos.getPosition();
		
		TestDeserializer td = new TestDeserializer( ts.buffers );
		
		TreeLazyLoader<TestPosition, TestDeserializer> tll = new TreeLazyLoader<TestPosition, TestDeserializer>( pos, td ); 
		
		SpanBuffer treeBuffer = tll.asSpanBuffer();
		
		assertEquals( text, treeBuffer.getText() );		
	}

	@Test
	public void testShort() throws IOException {
		TestSerializer ts = new TestSerializer();
		TreeOutputStream tos = new TreeOutputStream( ts );
		String text = "Now"; 
		tos.write( text.getBytes());
		tos.close();
		TestPosition pos = (TestPosition) tos.getPosition();
		
		assertEquals( 0, pos.idx );
		assertEquals( InnerNode.OUTER_NODE_FLAG, ts.buffers.get(0)[InnerNode.FLAG_BYTE] );
		
		TestDeserializer td = new TestDeserializer( ts.buffers );
		
		TreeLazyLoader<TestPosition, TestDeserializer> tll = new TreeLazyLoader<TestPosition, TestDeserializer>( pos, td ); 
		
		SpanBuffer treeBuffer = tll.asSpanBuffer();
		
		assertEquals( text, treeBuffer.getText() );
	}
	
	@Test
	public void testEmpty() throws IOException {
		TestSerializer ts = new TestSerializer();
		TreeOutputStream tos = new TreeOutputStream( ts );
		String text = ""; 
		tos.write( text.getBytes());
		tos.close();
		TestPosition pos = tos.getPosition();
		
		assertEquals( TestPosition.NO_DATA, pos );
		
		TestDeserializer td = new TestDeserializer( ts.buffers );
		
		TreeLazyLoader<TestPosition, TestDeserializer> tll = new TreeLazyLoader<TestPosition, TestDeserializer>( pos, td ); 
		
		SpanBuffer treeBuffer = tll.asSpanBuffer();
		
		assertEquals( text, treeBuffer.getText() );
		
	}
}
