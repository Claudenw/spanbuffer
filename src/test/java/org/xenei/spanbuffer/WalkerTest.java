package org.xenei.spanbuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
	String line="Hello again\n";
	short s = Short.MAX_VALUE / 2;
	byte ub = (byte)0xFE;
	int us = Short.MAX_VALUE + 1;
	String utf = "This is the UTF String";
	int skipLen = 10;
	byte[] buff = new byte[skipLen];
	
	SpanBuffer sb;
	
	public WalkerTest() throws IOException {
		try (
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream( baos );)
		{
		dos.write( b );
		dos.writeInt( i );
		dos.writeLong( l );
		dos.writeBoolean( bl );
		dos.writeChar( c );
		dos.writeDouble( d );
		dos.writeFloat( f );;
		dos.write( barry );;
		dos.writeBytes( line );
		dos.writeShort( s );
		dos.writeByte( ub );
		dos.writeShort( us );
		dos.writeUTF( utf );
		dos.write( buff );
		dos.write( b );
		dos.flush();
		ByteBuffer bb = ByteBuffer.wrap( baos.toByteArray() );
		sb = new SpanByteBuffer( bb );
		}
	}
	
	@Test
	public void testReads() throws IOException {
		Walker walker = sb.getWalker();
		assertEquals( b, walker.readByte());
		assertEquals( i, walker.readInt() );
		assertEquals( l, walker.readLong());
		assertEquals( bl, walker.readBoolean());
		assertEquals( c, walker.readChar());
		assertEquals( d, walker.readDouble(), 0.0);
		assertEquals( f, walker.readFloat(), 0.0f);
		byte[] ba = new byte[ barry.length ];
		assertEquals( barry.length, walker.read(ba));
		assertArrayEquals( barry, ba );
		assertEquals( "Hello again", walker.readLine());
		assertEquals( s, walker.readShort());
		assertEquals( 0xFE, walker.readUnsignedByte());
		assertEquals( Short.MAX_VALUE + 1, walker.readUnsignedShort());
		assertEquals( utf, walker.readUTF());
		assertEquals( skipLen, walker.skipBytes( skipLen ));
		assertEquals( b, walker.readByte());
	}

}
