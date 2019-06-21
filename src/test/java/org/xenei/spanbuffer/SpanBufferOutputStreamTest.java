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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

public class SpanBufferOutputStreamTest {

    @Test
    public void minimalTest() throws IOException {
        final SpanBufferOutputStream sbos = new SpanBufferOutputStream();
        sbos.write( "Hello World".getBytes() );
        SpanBuffer sb = sbos.getSpanBuffer();
        Assert.assertEquals( "Hello World", sb.getText() );

        sbos.write( "Hello World Again".getBytes() );
        sb = sbos.getSpanBuffer();
        Assert.assertEquals( "Hello WorldHello World Again", sb.getText() );

        sbos.close();
        try
        {
            sbos.getSpanBuffer();
            Assert.fail( "Should have thrown an already closed exception" );
        } catch (final IOException expected)
        {
            // do nothing expected result.
        }
    }

    @Test
    public void testLargeWrite() throws IOException {
        final String s = RandomStringUtils.randomAlphabetic( (int) FileUtils.ONE_MB );
        final SpanBufferOutputStream sbos = new SpanBufferOutputStream();
        for (int i = 0; i < 35; i++)
        {
            sbos.write( s.getBytes() );
        }
        final long size = s.getBytes().length * 35;
        final SpanBuffer sb = sbos.getSpanBuffer();
        Assert.assertEquals( size, sb.getLength() );
        final InputStream is = sb.getInputStream();
        final byte[] buffer = new byte[s.getBytes().length];

        while (is.read( buffer ) != -1)
        {
            Assert.assertEquals( s, new String( buffer ) );
        }
        sbos.close();
    }

}
