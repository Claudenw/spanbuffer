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

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractTest;
import org.xenei.junit.contract.IProducer;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.SpanBuffer;

@Contract(SpanBuffer.class)
public class SpanBufferContractTest<T extends SpanBuffer> {

	private static final long TestOffset = 5;

	public interface SpanBufferProducer<T extends SpanBuffer> extends IProducer<T> {
		public byte[] getBufferContents();
	}

	private SpanBufferProducer<T> producer;
	private SpanBuffer spanBuffer;
	private byte[] buffer;
	private byte missingByte;

	@Before
	public void setup() {
		spanBuffer = producer.newInstance();
		if (3 > spanBuffer.getLength()) {
			throw new IllegalArgumentException("Testing span must be at least 3 bytes long");
		}
		if (spanBuffer.getLength() >= Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Testing span must be less than " + Integer.MAX_VALUE + " long");
		}
		buffer = producer.getBufferContents();

		final BitSet bs = new BitSet(256);

		for (final int b : buffer) {
			bs.set(b & 0xFF);
		}

		final int missing = bs.nextClearBit(0);
		if (missing == -1) {
			throw new IllegalArgumentException("Testing span buffer must have at least one missing byte");
		}
		missingByte = (byte) missing;
	}

	@After
	public void cleanUp() {
		producer.cleanUp();
	}

	@Contract.Inject
	public final void setProducer(final SpanBufferProducer<T> producer) {
		this.producer = producer;
	}

	@ContractTest
	public void testSliceAtPosition() throws IOException {

		SpanBuffer span2 = spanBuffer.sliceAt(spanBuffer.getOffset());
		Assert.assertEquals("Sliced start value is incorrect", spanBuffer.getOffset(), span2.getOffset());
		Assert.assertEquals("Sliced end value is incorrect", spanBuffer.getEnd(), span2.getEnd());
		Assert.assertEquals("Sliced length value is incorrect", spanBuffer.getLength(), span2.getLength());
		byte[] buff = new byte[(int) span2.getLength()];
		Assert.assertEquals("Wong number of bytes read", span2.getLength(), span2.read(spanBuffer.getOffset(), buff));
		for (int i = 0; i < span2.getLength(); i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i]);
		}

		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		span2 = spanBuffer.sliceAt(mid);
		Assert.assertEquals("Sliced start value is incorrect", mid, span2.getOffset());
		Assert.assertEquals("Sliced end value is incorrect", LongSpan.calcEnd(span2), span2.getEnd());
		Assert.assertEquals("Sliced length value is incorrect", LongSpan.calcLength(span2), span2.getLength());
		buff = new byte[(int) span2.getLength()];

		Assert.assertEquals("Wong number of bytes read", span2.getLength(), span2.read(mid, buff));
		final int buffOff = (int) (spanBuffer.getLength() / 2);
		for (int i = 0; i < span2.getLength(); i++) {
			Assert.assertEquals("buffer contents wrong as position: " + i, buffer[buffOff + i], buff[i]);
		}
	}

	@ContractTest
	public void testGetInputStream() throws IOException {
		final InputStream stream = spanBuffer.getInputStream();
		Assert.assertNotNull(stream);

		for (final int b : buffer) {
			Assert.assertEquals("read wrong byte", (b & 0xFF), stream.read());
		}
		Assert.assertEquals("Should have returned -1", -1, stream.read());
	}

	@ContractTest
	public void testPositionOf() throws NoMatchException {

		final int mid = (buffer.length / 2);
		final byte[] b = new byte[1];
		b[0] = buffer[mid];
		int target = 0;
		for (int i = 0; i < buffer.length; i++) {
			if (buffer[i] == buffer[mid]) {
				target = i;
				break;
			}
		}
		SpanBuffer span2 = Factory.wrap(b);
		final long pos = spanBuffer.positionOf(span2);
		Assert.assertEquals("Should have located the target", target, pos);

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.positionOf(span2);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testPositionOfOnEmptyBuffer() {

		final SpanBuffer sb = spanBuffer.head(0);
		try {
			sb.positionOf(Factory.wrap("a"));
			Assert.fail("Should have thrown no match exception");
		} catch (final NoMatchException expected) {
			// do nothing
		}

	}

	@ContractTest
	public void testPositionOfLoc() throws IOException, NoMatchException {
		// find a byte that is duplicated in the buffer
		final Map<Byte, Integer> map = new HashMap<>();
		int firstPos = -1;
		int secondPos = -1;
		Byte b = null;
		for (int i = 0; i < buffer.length; i++) {
			b = Byte.valueOf(buffer[i]);
			if (map.containsKey(b)) {
				firstPos = map.get(b);
				secondPos = i;
				break;
			} else {
				map.put(b, Integer.valueOf(i));
			}
		}
		Assert.assertFalse("No duplicate bytes found in buffer", -1 == firstPos);

		SpanBuffer span2 = Factory.wrap(new byte[] { b.byteValue() });

		Assert.assertEquals("Should have found first instance", firstPos, spanBuffer.positionOf(span2, 0));
		Assert.assertEquals("Should have found first instance", firstPos, spanBuffer.positionOf(span2, firstPos));
		Assert.assertEquals("Should have found second instance", secondPos, spanBuffer.positionOf(span2, firstPos + 1));
		Assert.assertEquals("Should have found second instance", secondPos, spanBuffer.positionOf(span2, secondPos));

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.positionOf(span2, 0);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testPositionOfLocPastEndOfBuffer() throws NoMatchException {

		final SpanBuffer span2 = Factory.wrap("H");
		try {
			spanBuffer.positionOf(span2, spanBuffer.getEnd() + 1);
			Assert.fail("Should have thrown NoSuchElementException");
		} catch (final NoSuchElementException expected) {
			// do nothing
		}

	}

	@ContractTest
	public void testLastPositionOf() throws NoMatchException {
		final int mid = (buffer.length / 2);
		int target = 0;
		for (int i = buffer.length - 1; i > -1; i--) {
			if (buffer[i] == buffer[mid]) {
				target = i;
				break;
			}
		}
		SpanBuffer span2 = Factory.wrap(new byte[] { buffer[mid] });
		final long pos = spanBuffer.lastPositionOf(span2);
		Assert.assertEquals("Should have located the target", target, pos);

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.lastPositionOf(span2, 0);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testLastPositionOfEmptyNeedle() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY;
		final long pos = spanBuffer.lastPositionOf(span2);
		Assert.assertEquals("Should have located the target", spanBuffer.getEnd(), pos);
	}

	@ContractTest
	public void testLastPositionOfEmptyNeedlWithOffset() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY.duplicate(10);
		final long pos = spanBuffer.lastPositionOf(span2);
		Assert.assertEquals("Should have located the target", spanBuffer.getEnd(), pos);
	}

	@ContractTest
	public void testLastPositionOfWithNeedleOffset() throws NoMatchException {
		final int mid = (buffer.length / 2);
		int target = 0;
		for (int i = buffer.length - 1; i > -1; i--) {
			if (buffer[i] == buffer[mid]) {
				target = i;
				break;
			}
		}
		SpanBuffer span2 = Factory.wrap(10, new byte[] { buffer[mid] });
		final long pos = spanBuffer.lastPositionOf(span2);
		Assert.assertEquals("Should have located the target", target, pos);

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.lastPositionOf(span2, 0);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testLastPositionOfLocWithEmptyNeedle() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY;
		final long loc = spanBuffer.getEnd() - 1;
		final long pos = spanBuffer.lastPositionOf(span2, loc);
		Assert.assertEquals("Should have located the target", loc, pos);
	}

	@ContractTest
	public void testLastPositionOfLocWithEmptyNeedleWithOffset() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY.duplicate(10);
		final long loc = spanBuffer.getEnd() - 1;
		final long pos = spanBuffer.lastPositionOf(span2, loc);
		Assert.assertEquals("Should have located the target", loc, pos);
	}

	private class LastDuplicateInfo {
		public int firstPos;
		public int secondPos;
		public Byte duplicatedByte;

		public LastDuplicateInfo() {
			/*
			 * find a byte that is duplicated in the buffer retain first and second position
			 * of the byte and they byte itself.
			 */
			final Map<Byte, Integer> map = new HashMap<>();
			firstPos = -1;
			secondPos = -1;
			duplicatedByte = null;
			for (int i = buffer.length - 1; i > -1; i--) {
				duplicatedByte = Byte.valueOf(buffer[i]);
				if (map.containsKey(duplicatedByte)) {
					secondPos = map.get(duplicatedByte);
					if ((i + 1) == secondPos) {
						/*
						 * skip the case where they are adjacent. -1 will mean skip this byte
						 */
						map.put(duplicatedByte, Integer.valueOf(-1));

					} else {
						if (secondPos != -1) {
							firstPos = i;
							break;
						}
					}
				} else {
					map.put(duplicatedByte, Integer.valueOf(i));
				}
			}
			Assert.assertFalse("No duplicate bytes found in buffer", -1 == firstPos);
		}

		public SpanBuffer spanBuffer() {
			return Factory.wrap(new byte[] { duplicatedByte.byteValue() });
		}
	}

	@ContractTest
	public void testLastPositionOfLoc() throws IOException, NoMatchException {

		final LastDuplicateInfo duplicateInfo = new LastDuplicateInfo();
		SpanBuffer span2 = duplicateInfo.spanBuffer();
		try {
			spanBuffer.lastPositionOf(span2, 0);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
		Assert.assertEquals("Should have found first instance", duplicateInfo.firstPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.firstPos));
		Assert.assertEquals("Should have found first instance", duplicateInfo.firstPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.firstPos + 1));
		Assert.assertEquals("Should have found second instance", duplicateInfo.secondPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.secondPos));
		Assert.assertEquals("Should have found second instance", duplicateInfo.secondPos,
				spanBuffer.lastPositionOf(span2, Math.min(duplicateInfo.secondPos + 1, spanBuffer.getEnd())));

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.lastPositionOf(span2, spanBuffer.getEnd());
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testCommonPrefix() throws IOException {

		// shared prefix with extra bytes
		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		byte[] buff = new byte[mid + 3];
		System.arraycopy(buffer, 0, buff, 0, mid);
		buff[mid] = missingByte;
		buff[mid + 1] = missingByte;
		buff[mid + 2] = missingByte;
		SpanBuffer span2 = Factory.wrap(buff);
		long common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should have a common prefix", mid, common);

		// no match (null case)
		System.arraycopy(buffer, 1, buff, 0, mid);
		buff[mid] = missingByte;
		buff[mid + 1] = missingByte;
		buff[mid + 2] = missingByte;
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should not have a common prefix", 0, common);

		// entire case exact match with prefix
		buff = new byte[mid];
		System.arraycopy(buffer, 0, buff, 0, mid);
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should have a common prefix", mid, common);
	}

	@ContractTest
	public void testCommonSuffix() throws IOException {

		// prefix match
		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		final int len = buffer.length - mid;
		byte[] buff = new byte[len + 3];
		System.arraycopy(buffer, mid, buff, 3, len);
		buff[0] = missingByte;
		buff[1] = missingByte;
		buff[2] = missingByte;
		SpanBuffer span2 = Factory.wrap(buff);
		long common = spanBuffer.commonSuffix(span2);
		Assert.assertEquals("Should have a common suffix", len, common);

		// no match
		System.arraycopy(buffer, mid - 1, buff, 3, len);
		buff[0] = missingByte;
		buff[1] = missingByte;
		buff[2] = missingByte;
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonSuffix(span2);
		Assert.assertEquals("Should not have a common suffix", 0, common);

		// entire case exact match with suffix
		buff = new byte[len];
		System.arraycopy(buffer, mid, buff, 0, len);
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonSuffix(span2);
		Assert.assertEquals("Should have a common suffix", len, common);

	}

	@ContractTest
	public void testGetText() throws IOException {
		Assert.assertEquals(new String(buffer), spanBuffer.getText());
	}

	@ContractTest
	public void testMatch() throws NoMatchException, IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		SpanBuffer span2 = spanBuffer.sliceAt(mid);
		span2 = span2.head(span2.relativeUpperLimit(Matcher.MAX_BITAP));

		final Matcher matcher = spanBuffer.getMatcher();

		final Matcher.Result result = matcher.bitap(span2);
		Assert.assertEquals(mid, result.getIndex());

	}

	@ContractTest
	public void testMatchAtStart() throws NoMatchException, IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		SpanBuffer span2 = spanBuffer.head(mid);
		span2 = span2.head(span2.relativeUpperLimit(Matcher.MAX_BITAP));

		final Matcher matcher = spanBuffer.getMatcher();

		final Matcher.Result result = matcher.bitap(span2);
		Assert.assertEquals(spanBuffer.getOffset(), result.getIndex());
	}

	@ContractTest
	public void testReadPosition() throws IOException {
		for (int i = 0; i < spanBuffer.getLength(); i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], spanBuffer.read(i));
		}
	}

	@ContractTest
	public void testReadByteArray()  throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) mid];
		final int bytesRead = spanBuffer.read(0, buff);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i]);
		}
	}

	@ContractTest
	public void testReadByteArrayAtOffset()  throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) mid];
		final int bytesRead = spanBuffer.read(1, buff);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[1 + i], buff[i]);
		}
	}

	@ContractTest
	public void testReadByteArrayWithPos() throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) spanBuffer.getLength()];
		final int bytesRead = spanBuffer.read(0, buff, 1, (int) mid);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i + 1]);
		}
	}

	@ContractTest
	public void testReadByteArrayWithOffsetAndPos() throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) spanBuffer.getLength()];
		final int bytesRead = spanBuffer.read(1, buff, 1, (int) mid);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i + 1], buff[i + 1]);
		}
	}

	@ContractTest
	public void testSliceAtPositionWithOffset()  throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		testSliceAtPosition();
	}

	@ContractTest
	public void testGetInputStreamWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final InputStream stream = spanBuffer.getInputStream();
		Assert.assertNotNull(stream);
		for (int i = 0; i < buffer.length; i++) {
			Assert.assertEquals("read wrong byte", (buffer[i] & 0xFF), stream.read());
		}
		Assert.assertEquals("Should have returned -1", -1, stream.read());
	}

	@ContractTest
	public void testPositionOfWithOffset() throws NoMatchException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final int mid = (buffer.length / 2);
		final byte[] b = new byte[1];
		b[0] = buffer[mid];
		int target = 0;
		for (int i = 0; i < buffer.length; i++) {
			if (buffer[i] == buffer[mid]) {
				target = i;
				break;
			}
		}
		target += SpanBufferContractTest.TestOffset;

		SpanBuffer span2 = Factory.wrap(b);
		final long pos = spanBuffer.positionOf(span2);
		Assert.assertEquals("Should have located the target", target, pos);

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.positionOf(span2);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}

	}

	@ContractTest
	public void testPositionOfLocWithOffset() throws IOException, NoMatchException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		// find a byte that is duplicated in the buffer
		final Map<Byte, Integer> map = new HashMap<>();
		int firstPos = -1;
		int secondPos = -1;
		Byte b = null;
		for (int i = 0; i < buffer.length; i++) {
			b = Byte.valueOf(buffer[i]);
			if (map.containsKey(b)) {
				firstPos = map.get(b);
				secondPos = i;
				break;
			} else {
				map.put(b, Integer.valueOf(i));
			}
		}
		Assert.assertFalse("No duplicate bytes found in buffer", -1 == firstPos);
		firstPos = (int) spanBuffer.makeAbsolute(firstPos);
		secondPos = (int) spanBuffer.makeAbsolute(secondPos);

		SpanBuffer span2 = Factory.wrap(new byte[] { b.byteValue() });

		Assert.assertEquals("Should have found first instance", firstPos, spanBuffer.positionOf(span2));
		Assert.assertEquals("Should have found first instance", firstPos, spanBuffer.positionOf(span2, firstPos));
		Assert.assertEquals("Should have found second instance", secondPos, spanBuffer.positionOf(span2, firstPos + 1));
		Assert.assertEquals("Should have found second instance", secondPos, spanBuffer.positionOf(span2, secondPos));

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.positionOf(span2, spanBuffer.getOffset());
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testPositionOfLocWithEmptyNeedle() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY;
		final long pos = spanBuffer.positionOf(span2, 1);
		Assert.assertEquals("Should have located the target", 1, pos);
	}

	@ContractTest
	public void testPositionOfLocWithEmptyNeedleWithOffset() throws NoMatchException {
		final SpanBuffer span2 = Factory.EMPTY.duplicate(10);
		final long pos = spanBuffer.positionOf(span2);
		Assert.assertEquals("Should have located the target", 0, pos);
	}

	@ContractTest
	public void testLastPositionOfWithOffset() throws NoMatchException {
		final int mid = (buffer.length / 2);
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		int target = 0;
		for (int i = buffer.length - 1; i > -1; i--) {
			if (buffer[i] == buffer[mid]) {
				target = i;
				break;
			}
		}
		target += spanBuffer.getOffset();

		SpanBuffer span2 = Factory.wrap(new byte[] { buffer[mid] });
		final long pos = spanBuffer.lastPositionOf(span2);
		Assert.assertEquals("Should have located the target", target, pos);

		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.lastPositionOf(span2);
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testLastPositionOfLocWithOffset() throws IOException, NoMatchException {

		final LastDuplicateInfo duplicateInfo = new LastDuplicateInfo();

		SpanBuffer span2 = duplicateInfo.spanBuffer();

		// change the offset of the span buffer.
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		// adjust the first and second position based on the new offset.
		duplicateInfo.firstPos += spanBuffer.getOffset();
		duplicateInfo.secondPos += spanBuffer.getOffset();

		try {
			/*
			 * find the last position of the byte where we start at the beginning. so no
			 * value is possible
			 */
			spanBuffer.lastPositionOf(span2, spanBuffer.getOffset());
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
		/*
		 * The last position starting from the firstPosition should be the first
		 * position.
		 */
		Assert.assertEquals("Should have found first instance", duplicateInfo.firstPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.firstPos));
		/*
		 * The last position starting from the byte after the first position should be
		 * the first position
		 */
		Assert.assertEquals("Should have found first instance", duplicateInfo.firstPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.firstPos + 1));
		/*
		 * The last position starting from the second position should be the second
		 * position.
		 */
		Assert.assertEquals("Should have found second instance", duplicateInfo.secondPos,
				spanBuffer.lastPositionOf(span2, duplicateInfo.secondPos));
		/*
		 * The last position starting from the second position + 1 should be the sedond
		 * position
		 */
		Assert.assertEquals("Should have found second instance", duplicateInfo.secondPos,
				spanBuffer.lastPositionOf(span2, Math.min(duplicateInfo.secondPos + 1, spanBuffer.getEnd())));

		/*
		 * Check that the missing byte is not found in the buffer.
		 */
		span2 = Factory.wrap(new byte[] { missingByte });
		try {
			spanBuffer.lastPositionOf(span2, spanBuffer.getEnd());
			Assert.fail("Should have thrown NoMatchException");
		} catch (final NoMatchException expected) {
			// do nothing
		}
	}

	@ContractTest
	public void testCommonPrefixWithOffset() throws IOException {

		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		final byte[] buff = new byte[mid + 3];
		System.arraycopy(buffer, 0, buff, 0, mid);
		buff[mid] = missingByte;
		buff[mid + 1] = missingByte;
		buff[mid + 2] = missingByte;
		SpanBuffer span2 = Factory.wrap(buff);
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);

		long common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should have a common prefix", mid, common);

		System.arraycopy(buffer, 1, buff, 0, mid);
		buff[mid] = missingByte;
		buff[mid + 1] = missingByte;
		buff[mid + 2] = missingByte;
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should not have a common prefix", 0, common);

	}

	@ContractTest
	public void testCommonSuffixWithOffset() throws IOException {
		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));

		final int len = buffer.length - mid;
		final byte[] buff = new byte[len + 3];
		System.arraycopy(buffer, mid, buff, 3, len);
		buff[0] = missingByte;
		buff[1] = missingByte;
		buff[2] = missingByte;
		SpanBuffer span2 = Factory.wrap(buff);
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);

		long common = spanBuffer.commonSuffix(span2);
		Assert.assertEquals("Should have a common suffix", len, common);

		System.arraycopy(buffer, mid - 1, buff, 3, len);
		buff[0] = missingByte;
		buff[1] = missingByte;
		buff[2] = missingByte;
		span2 = Factory.wrap(buff);
		common = spanBuffer.commonPrefix(span2);
		Assert.assertEquals("Should not have a common prefix", 0, common);

	}

	@ContractTest
	public void testGetTextWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		Assert.assertEquals(new String(buffer), spanBuffer.getText());
	}

	@ContractTest
	public void testMatchWithOffset() throws NoMatchException, IOException {
		// absolute mid position
		long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		// get span buffer from mid with max length of
		SpanBuffer span2 = spanBuffer.sliceAt(mid);
		mid = spanBuffer.makeRelative(mid);
		span2 = span2.head(span2.relativeUpperLimit(Matcher.MAX_BITAP));

		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		mid = spanBuffer.makeAbsolute(mid);

		final Matcher matcher = spanBuffer.getMatcher();

		final Matcher.Result result = matcher.bitap(span2);
		Assert.assertEquals(mid, result.getIndex());
	}

	@ContractTest
	public void testMatchAtStartWithOffset() throws NoMatchException, IOException {
		final long mid = (spanBuffer.getLength() / 2);
		SpanBuffer span2 = spanBuffer.head(mid);
		span2 = span2.head(span2.relativeUpperLimit(Matcher.MAX_BITAP));

		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);

		final Matcher matcher = spanBuffer.getMatcher();

		final Matcher.Result result = matcher.bitap(span2);
		Assert.assertEquals(spanBuffer.getOffset(), result.getIndex());
	}

	@ContractTest
	public void testReadPositionWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		for (int i = 0; i < spanBuffer.getLength(); i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i],
					spanBuffer.read(i + SpanBufferContractTest.TestOffset));
		}
	}

	@ContractTest
	public void testReadByteArrayWithOffset() throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) mid];
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final int bytesRead = spanBuffer.read(SpanBufferContractTest.TestOffset, buff);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i]);
		}
	}

	@ContractTest
	public void testReadByteArrayAtOffsetWithOffset()  throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) mid];
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final int bytesRead = spanBuffer.read(SpanBufferContractTest.TestOffset + 1, buff);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[1 + i], buff[i]);
		}
	}

	@ContractTest
	public void testReadByteArrayWithPosAndOffset() throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) spanBuffer.getLength()];
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final int bytesRead = spanBuffer.read(SpanBufferContractTest.TestOffset, buff, 1, (int) mid);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i + 1]);
		}
	}

	@ContractTest
	public void testReadByteArrayWithOffsetAndPosAndOffset() throws IOException {
		final long mid = spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		final byte[] buff = new byte[(int) spanBuffer.getLength()];
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		final int bytesRead = spanBuffer.read(SpanBufferContractTest.TestOffset + 1, buff, 1, (int) mid);
		Assert.assertEquals("Should have read " + mid + " bytes", mid, bytesRead);
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i + 1], buff[i + 1]);
		}
	}

	@ContractTest
	public void testReadByteArrayWithExcessLength()  throws IOException {
		final byte[] buff = new byte[(int) spanBuffer.getLength() + 5];
		final int bytesRead = spanBuffer.read(0, buff);
		Assert.assertEquals("Should have read " + spanBuffer.getLength() + " bytes", spanBuffer.getLength(), bytesRead);
		for (int i = 0; i < spanBuffer.getLength(); i++) {
			Assert.assertEquals("buffer contents wrong", buffer[i], buff[i]);
		}
	}

	@ContractTest
	public void testStartsWith() throws IOException {
		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		SpanBuffer span2 = spanBuffer.head(mid);
		Assert.assertTrue("Should have starts with", spanBuffer.startsWith(span2));

		final byte[] buff = new byte[mid + 1];
		System.arraycopy(buffer, 1, buff, 0, mid);
		buff[0] = 0x20;
		span2 = Factory.wrap(buff);
		Assert.assertFalse("Should not have starts with", spanBuffer.startsWith(span2));
	}

	@ContractTest
	public void testEndsWith() throws IOException {

		final int mid = (int) spanBuffer.makeAbsolute((spanBuffer.getLength() / 2));
		SpanBuffer span2 = spanBuffer.sliceAt(mid);
		Assert.assertTrue("Should have ends with", spanBuffer.endsWith(span2));

		final byte[] buff = new byte[1];
		buff[0] = missingByte;
		span2 = Factory.wrap(buff);
		Assert.assertFalse("Should not have start with", spanBuffer.startsWith(span2));

	}

	@ContractTest
	public void testHead() throws IOException {
		SpanBuffer sb = spanBuffer.head(0);
		Assert.assertEquals(0, sb.getLength());

		sb = spanBuffer.head(1);
		Assert.assertEquals(1, sb.getLength());
		Assert.assertEquals(spanBuffer.getWalker().getByte(), sb.getWalker().getByte());

		sb = spanBuffer.head(buffer.length);
		Assert.assertEquals(buffer.length, sb.getLength());
		Walker walker = sb.getWalker();
		for (int i = 0; i < buffer.length; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}

		final int mid = buffer.length / 2;
		sb = spanBuffer.head(mid);
		Assert.assertEquals(mid, sb.getLength());
		walker = sb.getWalker();
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}
	}

	@ContractTest
	public void testHeadWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		testHead();
	}

	@ContractTest
	public void testTail() throws IOException {
		SpanBuffer sb = spanBuffer.tail(0);
		Assert.assertEquals(0, sb.getLength());

		sb = spanBuffer.tail(1);
		Assert.assertEquals(1, sb.getLength());
		Assert.assertEquals(buffer[buffer.length - 1], sb.getWalker().getByte());

		sb = spanBuffer.tail(buffer.length);
		Assert.assertEquals(buffer.length, sb.getLength());
		Walker walker = sb.getWalker();
		for (int i = 0; i < buffer.length; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}

		final int mid = buffer.length / 2;
		sb = spanBuffer.tail(mid);
		Assert.assertEquals(mid, sb.getLength());
		walker = sb.getWalker();
		final int bufferPos = buffer.length - mid;
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals(buffer[bufferPos + i], walker.getByte());
			walker.next();
		}

	}

	@ContractTest
	public void testTailWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		testTail();
	}

	@ContractTest
	public void testCut() throws IOException {

		SpanBuffer sb = spanBuffer.cut(0);
		Assert.assertEquals(buffer.length, sb.getLength());
		Walker walker = sb.getWalker();
		for (int i = 0; i < buffer.length; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}

		sb = spanBuffer.cut(1);
		Assert.assertEquals(buffer.length - 1, sb.getLength());
		walker = sb.getWalker();
		for (int i = 0; i < sb.getLength(); i++) {
			Assert.assertEquals(buffer[i + 1], walker.getByte());
			walker.next();
		}

		sb = spanBuffer.cut(buffer.length);
		Assert.assertEquals(0, sb.getLength());

		final int mid = buffer.length / 2;
		sb = spanBuffer.cut(mid);
		Assert.assertEquals(buffer.length - mid, sb.getLength());
		walker = sb.getWalker();
		for (int i = 0; i < sb.getLength(); i++) {
			Assert.assertEquals(buffer[i + mid], walker.getByte());
			walker.next();
		}
	}

	@ContractTest
	public void testCutWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		testCut();
	}

	@ContractTest
	public void testTrunc() throws IOException {

		SpanBuffer sb = spanBuffer.trunc(spanBuffer.getOffset());
		Assert.assertEquals(0, sb.getLength());

		sb = spanBuffer.trunc(1 + spanBuffer.getOffset());
		Assert.assertEquals(1, sb.getLength());
		Assert.assertEquals(spanBuffer.getWalker().getByte(), sb.getWalker().getByte());

		sb = spanBuffer.trunc(buffer.length + spanBuffer.getOffset());
		Assert.assertEquals(buffer.length, sb.getLength());
		Walker walker = sb.getWalker();
		for (int i = 0; i < buffer.length; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}

		final int mid = buffer.length / 2;
		sb = spanBuffer.trunc(mid + spanBuffer.getOffset());
		Assert.assertEquals(mid, sb.getLength());
		walker = sb.getWalker();
		for (int i = 0; i < mid; i++) {
			Assert.assertEquals(buffer[i], walker.getByte());
			walker.next();
		}
	}

	@ContractTest
	public void testTruncWithOffset() throws IOException {
		spanBuffer = spanBuffer.duplicate(SpanBufferContractTest.TestOffset);
		testTrunc();
	}

	@ContractTest
	public void testCutThenHead() throws IOException {
		final int mid = buffer.length / 2;
		spanBuffer = spanBuffer.cut(mid);
		spanBuffer = spanBuffer.head(1);
		Assert.assertEquals("Wrong byte", buffer[mid], spanBuffer.read(mid));
		Assert.assertEquals("Wrong offset", mid, spanBuffer.getOffset());
		Assert.assertEquals("wrong length", 1, spanBuffer.getLength());

	}

}
