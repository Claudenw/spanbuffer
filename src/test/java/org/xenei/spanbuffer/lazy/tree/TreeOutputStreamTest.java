package org.xenei.spanbuffer.lazy.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.junit.Test;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;
import org.xenei.spanbuffer.lazy.tree.node.HeapBufferFactory;

public class TreeOutputStreamTest {

	private TestSerializer ts = new TestSerializer();
	private BufferFactory factory = new HeapBufferFactory( ts.getMaxBufferSize());

	@Test
	public void writeTest() throws IOException {
		TreeOutputStream tos = new TreeOutputStream(ts, factory);
		String text = "Now is the time for all good men to come to the aid of their country";
		tos.write(text.getBytes());
		tos.close();
		TestPosition pos = (TestPosition) tos.getPosition();
		assertEquals( 13, pos.idx );
		
		assertEquals( 14, ts.buffers.size());
		assertText( 0, "Now is the");
		assertText( 1, " time for ");
		assertText( 2, "all good m");
		assertText( 4, "en to come");
		assertText( 5, " to the ai");
		assertText( 7, "d of their");
		assertText( 8, " country");
		assertPtr( 3, 0, 0, 1);
		assertPtr( 6, 0, 2, 4);
		assertPtr( 9, 0, 5, 7);
		assertPtr( 10, 1, 3, 6);
		assertPtr( 11, 0, 8);
		assertPtr( 12, 1, 9, 11);
		assertPtr( 13, 1, 10, 12);

	}
	
	private void assertText( int idx, String txt)
	{
		ByteBuffer actual = ts.buffers.get(idx);
		ByteBuffer expected = ByteBuffer.wrap( txt.getBytes() );
		assertEquals( txt, expected, actual );
	}
	
	private void assertPtr( int idx, int type, int...ptrs)
	{
		ByteBuffer actual = ts.buffers.get(idx);
		assertEquals( "wrong type", type, actual.get(0));
		IntBuffer ints = actual.position(1).asIntBuffer();
		if (ptrs.length == 1)
		{
			assertEquals( ptrs[0], ints.get(0));
			assertEquals( 5, actual.limit() );
		}
		else if (ptrs.length==2) {
			assertEquals( ptrs[0], ints.get(0));
			assertEquals( ptrs[1], ints.get(1));
			assertEquals( 9, actual.limit() );
		}
		else {
			fail( "Wrong number of ptrs: "+ptrs.length);
		}
	}


}
