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
package org.xenei.spanbuffer.streams;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A SpanBufferOutputStream that writes to memory until max mem size is exceeded after which it writes to
 *  a temp file.
 * The resulting SpanBuffer will be memory based if small enough or file based otherwise.
 * 
 * calling getSpanBuffer() after the stream is closed will return the SpanBuffer.
 *
 * @see Factory#getMaxMemBuffer
 */
public class SpanBufferOutputStream extends OutputStream {
    private final ByteArrayOutputStream baos;
    private FileOutputStream fos;
    private File file;
    private boolean closed;
    private static final Logger LOG = LoggerFactory.getLogger( SpanBufferOutputStream.class );

    /**
     * Create empty SpanBufferOutputStream.
     */
    public SpanBufferOutputStream() {
        baos = new ByteArrayOutputStream();
        fos = null;
        closed = false;
        file = null;
    }

    @Override
    public void write(final int arg0) throws IOException {
        if (closed)
        {
            throw new IOException( "SpanBuffer output stream closed" );
        }
        baos.write( arg0 );
        if (baos.size() >= Factory.getMaxMemBuffer())
        {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        baos.close();
        if (fos != null)
        {
            fos.close();
            if (file != null)
            {
                if (!file.delete())
                {
                    SpanBufferOutputStream.LOG.warn( " Unable to delete: " + file.getAbsolutePath() );
                }
            }
        }
        closed = true;
    }

    @Override
    public void flush() throws IOException {

        if (closed)
        {
            throw new IOException( "SpanBuffer output stream closed" );
        }

        baos.flush();

        if (baos.size() > 0)
        {
            if ((fos == null) && (baos.size() >= Factory.getMaxMemBuffer()))
            {
                file = File.createTempFile( "sb-", ".tmp" );
                fos = new FileOutputStream( file );
            }
            if (fos != null)
            {
                fos.write( baos.toByteArray() );
                baos.reset();
            }
        }

        if (fos != null)
        {
            fos.flush();
        }
    }

    /**
     * Get the span buffer from the stream.
     *
     * @return The span buffer.
     * @throws FileNotFoundException
     *             on disk error
     * @throws IOException
     *             on general IO error.
     */
    public SpanBuffer getSpanBuffer() throws FileNotFoundException, IOException {

        if (closed)
        {
            throw new IOException( "SpanBuffer output stream closed" );
        }

        if (file != null)
        {
            flush();
            return Factory.wrap( file );
        }

        return Factory.wrap( baos.toByteArray() );
    }

}