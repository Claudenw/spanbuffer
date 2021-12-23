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

import org.xenei.spanbuffer.similarity.Bitap;

/**
 * Matcher provides some convenience functions to perform matching from a
 * specific location in a buffer.
 */
public class Matcher {

    /**
     * Match a pattern starting at the specified relative position.
     * <p>
     * Characters before loc will not be considered in the match
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
     * </p>
     * Absolute version of this method is match()
     *
     * @param pattern  The pattern to match.
     * @param position The relative position to start matching at
     * @return The relative match position.
     * @throws NoMatchException if match not found
     * @throws IOException      ion IOerror
     */
    public Bitap.Result matchFrom(final SpanBuffer target, final SpanBuffer pattern, final long loc)
            throws NoMatchException, IOException {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        return match(target, pattern, target.makeAbsolute(loc));
    }

    /**
     * Get a match result starting from a relative position.
     * <p>
     * Characters before loc will not be considered in the match
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
     * </p>
     * Absolute version of this method is match()
     *
     * @param target  the target find the pattern in.
     * @param pattern the pattern to match
     * @param start   the location to start the match from.
     * @param config  the Bitap.Config to use for the match.
     * @return The match result.
     * @throws NoMatchException if a match could not be established
     * @throws IOException      on IO error.
     */
    public static Bitap.Result matchFrom(final SpanBuffer target, final SpanBuffer pattern, final long start,
            Bitap.Config config) throws NoMatchException, IOException {
        return match(target, pattern, target.makeAbsolute(start), config);
    }

    /**
     * Get a match result starting from the absolute beginning of the target.
     *
     * @param target  the target find the pattern in.
     * @param pattern the pattern to match
     * @return The match result.
     * @throws NoMatchException if a match could not be established
     * @throws IOException      on IO error.
     */
    public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern)
            throws NoMatchException, IOException {
        return match(target, pattern, target.getOffset());
    }

    /**
     * Get a match result starting from a previous match.
     *
     * @param target     the target find the pattern in.
     * @param pattern    the pattern to match
     * @param prevResult A previous result.
     * @return The match result.
     * @throws NoMatchException if a match could not be established
     * @throws IOException      on IO error.
     */
    public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final Bitap.Result prevResult)
            throws NoMatchException, IOException {
        return match(target, pattern, prevResult.getAbsIndex() + 1, prevResult.getConfig());
    }

    /**
     * Get a match result starting from a specific absolute position.
     * <p>
     * Characters before start will not be considered in the match
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
     * </p>
     *
     * @param target  the target find the pattern in.
     * @param pattern the pattern to match
     * @param start   the location to start the match from.
     * @return The match result.
     * @throws NoMatchException if a match could not be established
     * @throws IOException      on IO error.
     */
    public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final long start)
            throws NoMatchException, IOException {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("pattern may not be null");
        }

        // Perfect match at the perfect spot! (Includes case of null pattern)
        final SpanBuffer subTarget = target.sliceAt(start);
        Bitap.Result result = null;
        if (subTarget.startsWith(pattern)) {
            result = new Bitap.Result(start, target.makeRelative(start));
        } else {
            Bitap bitap = new Bitap();
            result = bitap.execute(subTarget, pattern, subTarget.getOffset());
            if (result == null) {
                throw new NoMatchException();
            }
        }
        return result;
    }

    /**
     * Get a match result starting from a specific absolute position.
     * <p>
     * Characters before start will not be considered in the match
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
     * </p>
     *
     * @param target  the target find the pattern in.
     * @param pattern the pattern to match
     * @param start   the location to start the match from.
     * @param config  the Bitap.Config to use for the match.
     * @return The match result.
     * @throws NoMatchException if a match could not be established
     * @throws IOException      on IO error.
     */
    public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final long start,
            Bitap.Config config) throws NoMatchException, IOException {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("pattern may not be null");
        }

        // Perfect match at the perfect spot! (Includes case of null pattern)
        final SpanBuffer subTarget = target.sliceAt(start);
        Bitap.Result result = null;
        if (subTarget.startsWith(pattern)) {
            result = new Bitap.Result(config, start, target.makeRelative(start));
        } else {
            Bitap bitap = new Bitap(config);
            result = bitap.execute(subTarget, pattern, subTarget.getOffset());

            if (result == null) {
                throw new NoMatchException();
            }
        }
        return result;
    }

}