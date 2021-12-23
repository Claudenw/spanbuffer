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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.span.NumberUtils;
import org.xenei.spanbuffer.impl.SpanBufferList;
import org.xenei.spanbuffer.impl.SpanByteBuffer;

/*
 * FileChannel and RandomAccessFile tests are covered by
 * the File tests as is makes calls through those methods
 */

public class FactoryTests {

    @Before
    public void setup() {
        Factory.setMaxHeap(NumberUtils.checkIntLimit("MAX_HEAP", 32 * FileUtils.ONE_MB));
    }

    @Test
    public void testMergeEmptyFront() throws IOException {
        final SpanBuffer sb = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.merge(Factory.EMPTY, sb);
        Assert.assertEquals("Hello", sb2.getText());
        Assert.assertEquals(0, sb2.getOffset());
        Assert.assertEquals(5, sb2.getLength());
        Assert.assertTrue(sb2 instanceof SpanByteBuffer);
    }

    @Test
    public void testMergeEmptyBack() throws IOException {
        final SpanBuffer sb = Factory.wrap("Hello");

        SpanBuffer sb2 = Factory.merge(sb, Factory.EMPTY);
        Assert.assertEquals("Hello", sb2.getText());
        Assert.assertEquals(0, sb2.getOffset());
        Assert.assertEquals(5, sb2.getLength());
        Assert.assertTrue(sb2 instanceof SpanByteBuffer);
    }

    @Test
    public void testMerge() throws IOException {
        SpanBuffer sb = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.merge(sb, sb);
        Assert.assertTrue(sb2 instanceof SpanBufferList);
        Assert.assertEquals("HelloHello", sb2.getText());
        Assert.assertEquals(0, sb2.getOffset());
        Assert.assertEquals(10, sb2.getLength());
        Assert.assertTrue(sb2 instanceof SpanBufferList);
    }

    @Test
    public void testMergeIterator() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");
        Iterator<SpanBuffer> iter = Arrays.asList(sb1, sb2, sb3).iterator();

        SpanBuffer sb = Factory.merge(iter);
        Assert.assertEquals("Hello World", sb.getText());
        Assert.assertEquals(0, sb.getOffset());
        Assert.assertEquals("Hello World".length(), sb.getLength());
    }

    @Test
    public void testMergeIteratorWithOffset() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");
        Iterator<SpanBuffer> iter = Arrays.asList(sb1, sb2, sb3).iterator();

        SpanBuffer sb = Factory.merge(5, iter);
        Assert.assertEquals(5, sb.getOffset());
        Assert.assertEquals("Hello World", sb.getText());

    }

    @Test
    public void testMergeList() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");

        SpanBuffer sb = Factory.merge(sb1, sb2, sb3);
        Assert.assertEquals("Hello World", sb.getText());
        Assert.assertEquals(0, sb.getOffset());
        Assert.assertEquals("Hello World".length(), sb.getLength());
    }

    @Test
    public void testMergeListWithOffset() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");

        SpanBuffer sb = Factory.merge(5, sb1, sb2, sb3);
        Assert.assertEquals(5, sb.getOffset());
        Assert.assertEquals("Hello World", sb.getText());

    }

    @Test
    public void testMergeStream() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");
        List<SpanBuffer> lst = Arrays.asList(sb1, sb2, sb3);

        SpanBuffer sb = Factory.merge(lst.stream());
        Assert.assertEquals("Hello World", sb.getText());
        Assert.assertEquals(0, sb.getOffset());
        Assert.assertEquals("Hello World".length(), sb.getLength());
    }

    @Test
    public void testMergeStreamWithOffset() throws IOException {
        SpanBuffer sb1 = Factory.wrap("Hello");
        SpanBuffer sb2 = Factory.wrap(" ");
        SpanBuffer sb3 = Factory.wrap("World");
        List<SpanBuffer> lst = Arrays.asList(sb1, sb2, sb3);

        SpanBuffer sb = Factory.merge(5, lst.stream());
        Assert.assertEquals(5, sb.getOffset());
        Assert.assertEquals("Hello World", sb.getText());

    }

    @Test
    public void testSetMaxHeap() {
        int initialSize = Factory.MAX_HEAP;
        Assert.assertEquals("Wrong initial value", Factory.MAX_HEAP, Factory.getMaxHeap());
        int newSize = Factory.MAX_HEAP / 2;
        int oldSize = Factory.setMaxHeap(newSize);
        Assert.assertEquals("Wrong return value", initialSize, oldSize);
        Assert.assertEquals("Wrong current value", newSize, Factory.getMaxHeap());
    }

    @Test
    public void testWrapBytes() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        SpanBuffer sb = Factory.wrap(buff);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("wrong absolute byte value", i, sb.read(i));
            Assert.assertEquals("wrong relative byte value", i, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapBytesOffset() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        SpanBuffer sb = Factory.wrap(5, buff);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("wrong absolute byte value", i, sb.read(5 + i));
            Assert.assertEquals("wrong relative byte value", i, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapBytesSubString() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        SpanBuffer sb = Factory.wrap(buff, 2, 2);
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals("wrong absolute byte value", i + 2, sb.read(i));
            Assert.assertEquals("wrong relative byte value", i + 2, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapBytesSubStringOffset() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        SpanBuffer sb = Factory.wrap(15, buff, 1, 2);
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals("wrong absolute byte value", i + 1, sb.read(15 + i));
            Assert.assertEquals("wrong relative byte value", i + 1, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapByteBuffer() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        ByteBuffer bb = ByteBuffer.wrap(buff);
        SpanBuffer sb = Factory.wrap(bb);
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals("wrong absolute byte value", i, sb.read(i));
            Assert.assertEquals("wrong relative byte value", i, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapByteBufferOffset() throws IOException {
        byte[] buff = new byte[5];
        for (int i = 0; i < 5; i++) {
            buff[i] = (byte) i;
        }
        ByteBuffer bb = ByteBuffer.wrap(buff);
        SpanBuffer sb = Factory.wrap(15, bb);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("wrong absolute byte value", i, sb.read(15 + i));
            Assert.assertEquals("wrong relative byte value", i, sb.readRelative(i));
        }
    }

    @Test
    public void testWrapCharSequence() throws IOException {
        CharSequence cs = new StringBuilder().append("Hello World");

        SpanBuffer sb = Factory.wrap(cs);
        Assert.assertEquals("Hello World", sb.getText());
        Assert.assertEquals(0, sb.getOffset());
        Assert.assertEquals("Hello World".length(), sb.getLength());

    }

    @Test
    public void testWrapCharSequenceOffset() throws IOException {
        CharSequence cs = new StringBuilder().append("Hello World");

        SpanBuffer sb = Factory.wrap(5, cs);
        Assert.assertEquals("Hello World", sb.getText());
        Assert.assertEquals(5, sb.getOffset());
        Assert.assertEquals("Hello World".length(), sb.getLength());

    }

    private String findFile(String fileName) {
        URL url = this.getClass().getClassLoader().getResource(fileName);
        return url.getFile();
    }

    @Test
    public void testWrapFile() throws IOException {
        File file = new File(findFile("small.txt"));
        SpanBuffer sb = Factory.wrap(file);
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }

    }

    @SuppressWarnings("unused")
    @Test
    public void testWrapFileDelete() throws IOException, InterruptedException {
        File file = new File(findFile("small.txt"));
        File otherFile = File.createTempFile("small", "txt");
        try (InputStream in = new FileInputStream(file); OutputStream out = new FileOutputStream(otherFile)) {
            IOUtils.copyLarge(in, out);
        }
        SpanBuffer sb = Factory.wrap(otherFile, true);
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }
        // alloc 2 huge blocks so that gc will run.
        long limit = Runtime.getRuntime().freeMemory();
        byte[] huge = new byte[(int) limit];
        limit = Runtime.getRuntime().freeMemory();
        byte[] huge2 = new byte[(int) limit];
        System.gc();
        Thread.sleep(100); // time for thread to run
        Assert.assertFalse(otherFile.getAbsolutePath() + " should not exist", otherFile.exists());
    }

    @Test
    public void testWrapFileOnHeapLazyLoader() throws IOException, InterruptedException {
        File file = new File(findFile("small.txt"));

        // 100 K buffer (file is approx 369K)
        int bufferSize = (int) (100 * FileUtils.ONE_KB);
        SpanBuffer sb = Factory.wrap(file, bufferSize);
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }
        sb = null;
    }

    @Test
    public void testWrapFileOffHeapLazyLoader() throws IOException, InterruptedException {
        File file = new File(findFile("small.txt"));

        /*
         * set MAX_HEAP small enough that this file will trigger OffHeapLazyLoader
         */
        Factory.setMaxHeap((int) (file.length() / 2));
        // 100 K buffer (file is approx 369K)
        int bufferSize = (int) (100 * FileUtils.ONE_KB);
        SpanBuffer sb = Factory.wrap(file, bufferSize, false);
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }
        sb = null;
    }

    @Test
    public void testWrapInputStream() throws IOException {
        File file = new File(findFile("small.txt"));
        SpanBuffer sb = null;
        try (InputStream in = new FileInputStream(file)) {
            sb = Factory.wrap(in);
        }

        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }
    }

    @Test
    public void testWrapFileString() throws IOException {
        File file = new File(findFile("small.txt"));
        SpanBuffer sb = Factory.wrapFile(findFile("small.txt"));
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }

    }

    @Test
    public void testAsMemMap() throws IOException {
        File file = new File(findFile("small.txt"));
        int bufferSize = (int) (100 * FileUtils.ONE_KB);
        SpanBuffer sb = Factory.asMemMap(file, bufferSize);
        Assert.assertEquals(file.length(), sb.getLength());
        Assert.assertEquals(0, sb.getOffset());
        try (InputStream in = new FileInputStream(file)) {
            for (int i = 0; i < file.length(); i++) {
                Assert.assertEquals(in.read(), (0xFF) & sb.read(i));
            }
        }

    }

}
