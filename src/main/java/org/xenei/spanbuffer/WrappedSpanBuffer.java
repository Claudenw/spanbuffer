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

import org.xenei.spanbuffer.streams.SpanBufferInputStream;

/**
 * An abstract class that wraps another SpanBuffer.
 *
 * <p>
 * This is generally used to add functionality to an existing SpanBuffer implementation.
 * </p>
 */
public abstract class WrappedSpanBuffer implements SpanBuffer {
    private final SpanBuffer sb;

    /**
     * Constructor.
     * <p>
     * If the <code>sb</code> parameter is a WrappedSpanBuffer instance its inner buffer is located and wrapped.
     * </p>
     *
     * @param sb
     *            The span buffer to wrap
     */
    public WrappedSpanBuffer(SpanBuffer sb) {
        while (sb instanceof WrappedSpanBuffer)
        {
            sb = ((WrappedSpanBuffer) sb).sb;
        }
        this.sb = sb;
    }

    /**
     * Get the innerspan buffer.
     *
     * @deprecated use {@link #getBuffer()}
     * @return the inner span buffer.
     */
    @Deprecated
    protected SpanBuffer getInnerSpanBuffer() {
        if (sb instanceof WrappedSpanBuffer)
        {
            return ((WrappedSpanBuffer) sb).getInnerSpanBuffer();
        }
        return sb;
    }

    @Override
    public String toString() {
        return SpanBuffer.Utils.simpleText( this );
    }

    /**
     * Get the innermost wrapped buffer.
     *
     *
     * @return The buffer this instance is wrapping.
     */
    public SpanBuffer getBuffer() {
        return sb;
    }

    @Override
    public long getOffset() {
        return sb.getOffset();
    }

    
    @Override
    public long getLength() {
        return sb.getLength();
    }

    @Override
    public SpanBufferInputStream getInputStream() {
        return sb.getInputStream();
    }

    @Override
    public long getEnd() {
        return sb.getEnd();
    }

    @Override
    public long positionOf(final SpanBuffer needle, final long loc) throws NoMatchException {
        return sb.positionOf( needle, loc );
    }

    @Override
    public long positionOf(final SpanBuffer needle) throws NoMatchException {
        return sb.positionOf( needle );
    }

    @Override
    public boolean contains(final long pos) {
        return sb.contains( pos );
    }

    @Override
    public long lastPositionOf(final SpanBuffer needle, final long pos) throws NoMatchException {
        return sb.lastPositionOf( needle, pos );
    }

    @Override
    public long lastPositionOf(final SpanBuffer needle) throws NoMatchException {
        return sb.lastPositionOf( needle );
    }

    @Override
    public long commonPrefix(final SpanBuffer buffer2) throws IOException {
        return sb.commonPrefix( buffer2 );
    }

    @Override
    public long commonSuffix(final SpanBuffer buffer2) throws IOException {
        return sb.commonSuffix( buffer2 );
    }

    @Override
    public String getText() throws IOException {
        return sb.getText();
    }

    @Override
    public Matcher getMatcher() {
        return sb.getMatcher();
    }

    @Override
    public byte read(final long position) throws IOException {
        return sb.read( position );
    }

    @Override
    public int read(final long position, final byte[] buff) {
        return sb.read( position, buff );
    }

    @Override
    public int read(final long position, final byte[] buff, final int pos, final int len) {
        return sb.read( position, buff, pos, len );
    }

    @Override
    public Walker getWalker(final long position) {
        return sb.getWalker( position );
    }

    @Override
    public Walker getWalker() {
        return sb.getWalker();
    }

    @Override
    public boolean startsWith(final SpanBuffer other) throws IOException {
        return sb.startsWith( other );
    }

    @Override
    public boolean endsWith(final SpanBuffer other) throws IOException {
        return sb.endsWith( other );
    }

    @Override
    public SpanBuffer safeSliceAt(final long position) {
        return sb.safeSliceAt( position );
    }

    @Override
    public String getHex() throws IOException {
        return sb.getHex();
    }

    @Override
    public String getHex(final int limit) throws IOException {
        return sb.getHex( limit );
    }

    @Override
    public byte readRelative(final long byteOffset) throws IOException {
        return sb.readRelative( byteOffset );
    }

    @Override
    public int readRelative(final long byteOffset, final byte[] buff) {
        return sb.readRelative( byteOffset, buff );
    }

    @Override
    public int readRelative(final long byteOffset, final byte[] buff, final int pos, final int len) {
        return sb.readRelative( byteOffset, buff, pos, len );
    }

}
