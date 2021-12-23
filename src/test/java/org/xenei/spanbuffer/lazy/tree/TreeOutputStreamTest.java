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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.xenei.spanbuffer.lazy.tree.TestSerde.TestSerializer;
import org.xenei.spanbuffer.lazy.tree.node.HeapBufferFactory;

@RunWith(Parameterized.class)
public class TreeOutputStreamTest {
    private TestSerde serde;
    private List<ByteBuffer> buffers;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        TestSerde ts = new TestSerde(new HeapBufferFactory(10));
        TestSerde bts = new TestSerde(new TestHeaderBufferFactory(10));
        return Arrays.asList(new Object[][] { { "No Offset", ts }, { "Offset", bts } });
    }

    public TreeOutputStreamTest(String name, TestSerde serde) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        this.serde = serde;
        this.buffers = ((TestSerializer) serde.getSerializer()).buffers;
    }

    @Test
    public void writeTest() throws IOException {
        TreeOutputStream tos = new TreeOutputStream(serde);
        String text = "Now is the time for all good men to come to the aid of their country";
        tos.write(text.getBytes());
        tos.close();
        TestPosition pos = (TestPosition) tos.getPosition();
        assertEquals(13, pos.idx);

        assertEquals(14, buffers.size());
        assertText(0, "Now is the");
        assertText(1, " time for ");
        assertText(2, "all good m");
        assertText(4, "en to come");
        assertText(5, " to the ai");
        assertText(7, "d of their");
        assertText(8, " country");
        assertPtr(3, 0, 0, 1);
        assertPtr(6, 0, 2, 4);
        assertPtr(9, 0, 5, 7);
        assertPtr(10, 1, 3, 6);
        assertPtr(11, 0, 8);
        assertPtr(12, 1, 9, 11);
        assertPtr(13, 1, 10, 12);

    }

    private void assertText(int idx, String txt) {
        ByteBuffer actual = buffers.get(idx);
        ByteBuffer trimmed = verifyHeader(idx, actual);
        ByteBuffer expected = ByteBuffer.wrap(txt.getBytes());
        assertEquals(String.format("[%s]", txt), expected, trimmed);
    }

    /**
     * Verify the header is correct and then position the buffer after the header.
     * Used for testing to verify header not damaged.
     *
     * @param idx    the buffer number
     * @param buffer the buffer.
     * @return the buffer positioned after header.
     */
    private ByteBuffer verifyHeader(int idx, ByteBuffer buffer) {

        if (serde.getFactory().headerSize() > 0) {
            for (int i = 0; i < serde.getFactory().headerSize(); i++) {
                assertEquals(String.format("buffer (%s) header corrupted at %s", idx, i), (byte) i, buffer.get(i));
            }
            buffer.flip();
            buffer.position(serde.getFactory().headerSize());
            return buffer;
        }
        buffer.flip();
        return buffer;
    }

    private void assertPtr(int idx, int type, int... ptrs) {
        ByteBuffer actual = buffers.get(idx);
        ByteBuffer trimmed = verifyHeader(idx, actual);
        assertEquals("wrong type", type, trimmed.get());
        IntBuffer ints = trimmed.asIntBuffer();
        if (ptrs.length == 1) {
            assertEquals(ptrs[0], ints.get(0));
            assertEquals(4, trimmed.remaining());
        } else if (ptrs.length == 2) {
            assertEquals(ptrs[0], ints.get(0));
            assertEquals(ptrs[1], ints.get(1));
            assertEquals(8, trimmed.remaining());
        } else {
            fail("Wrong number of ptrs: " + ptrs.length);
        }
    }

}
