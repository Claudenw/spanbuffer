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

/**
 * A class that performs matching tasks on a SpanBuffer.
 */
public interface Matcher {
    /**
     * The maximum size of a pattern for a BITAP match.
     */
    public static final int MAX_BITAP = Integer.SIZE;
    /**
     * The default match distance used by the match function.
     */
    public static final int DEFAULT_MATCH_DISTANCE = 1000;
    /**
     * The default match threshold used by the match function.
     */
    public static final double DEFAULT_MATCH_THRESHOLD = 0.5d;

    /**
     * Match a pattern starting at the specified relative position.
     *
     * <p>
     * See discussion of Absolute and Relative methods above.
     * </p>
     *
     * @param pattern
     *            The pattern to match.
     * @param position
     *            The relative position to start matching at
     * @return The relative match position.
     * @throws NoMatchException
     *             if match not found
     * @throws IOException
     *             ion IOerror
     */
    long matchFrom(final SpanBuffer pattern, long position) throws NoMatchException, IOException;

    /**
     * Get a match result from an attempted match.
     *
     * @param pattern
     *            the pattern to match
     * @return The match result.
     * @throws NoMatchException
     *             if a match could not be established
     * @throws IOException
     *             on IO error.
     */
    public Matcher.Result match(final SpanBuffer pattern) throws NoMatchException, IOException;

    /**
     * Get a match result from an attempted match.
     *
     * @param pattern
     *            the pattern to match
     * @param loc
     *            the location to start the match from.
     * @return The match result.
     * @throws NoMatchException
     *             if a match could not be established
     * @throws IOException
     *             on IO error.
     */
    public Matcher.Result match(final SpanBuffer pattern, long loc) throws NoMatchException, IOException;

    /**
     * Set the threshold for a match. Default value is 0.5
     *
     * @param threshold
     *            the new threshold.
     * @return the old threshold.
     */
    double setThreshold(double threshold);

    /**
     * Set the distance for a match.
     *
     * @param distance
     *            The new distance value
     * @return the old distance value
     */
    int setDistance(int distance);

    /**
     * <p>
     * Find the Levenshtein distance between two SpanBuffers.
     * </p>
     *
     * <p>
     * This is the number of changes needed to change one buffer into another, where each change is a single byte
     * modification (deletion, insertion or substitution).
     * </p>
     *
     * @param pattern
     *            the pattern to get the distance to.
     * @return result distance
     * @throws IllegalArgumentException
     *             if either buffer is longer than Integer.MAX_VALUE-1
     */
    int getLevenshteinDistance(final SpanBuffer pattern);

    /**
     * <p>
     * Find the Levenshtein distance between two buffers if it's less than or equal to a given threshold.
     * </p>
     *
     * <p>
     * This is the number of changes needed to change one buffer into another, where each change is a single byte
     * modification (deletion, insertion or substitution).
     * </p>
     *
     * <p>
     * This implementations is based on Apache Commons 3.5 StringUtils.getLevenshteinDistance
     * </p>
     *
     * @param pattern
     *            The pattern to get the distance to.
     * @param threshold
     *            the target threshold, must not be negative
     * @return result distance, or {@code -1} if the distance would be greater than the threshold
     * @throws IllegalArgumentException
     *             if negative threshold
     */
    int getLevenshteinDistance(final SpanBuffer pattern, final int threshold);

    /**
     * <p>
     * Find the Fuzzy Distance which indicates the similarity score between two buffers.
     * </p>
     *
     * <p>
     * This buffer matching algorithm is similar to the algorithms of editors such as Sublime Text, TextMate, Atom
     * and others. One point is given for every matched byte. Subsequent matches yield two bonus points. A higher
     * score indicates a higher similarity.
     * </p>
     *
     * @param pattern
     *            the pattern to get the distance to.
     * @return result score
     */
    int getFuzzyDistance(final SpanBuffer pattern);

    /**
     * Calculate the bitap distance. The tolerance is the number of errors acceptable in the matched string.
     *
     * <p>
     * The resulting position (in the Result object) is relative.
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods above.
     * </p>
     *
     * @param pattern
     *            The pattern to match
     * @return The result object representing match or null if no match was found.
     * @throws IOException
     *             on error
     */
    Matcher.Result bitap(SpanBuffer pattern) throws IOException;

    /**
     * Calculate the bitap distance. The tolerance is the number of errors acceptable in the matched string.
     *
     * <p>
     * The resulting position (in the Result object) is relative.
     *
     * </p>
     * <p>
     * See discussion of Absolute and Relative methods above.
     * </p>
     *
     * @param pattern
     *            The pattern to match
     * @param loc
     *            The absolute location to start the pattern match from.
     * @return The result object representing match or null if no match was found.
     * @throws IOException
     *             on error
     */
    Matcher.Result bitap(SpanBuffer pattern, long loc) throws IOException;

    /**
     * The result of a bitap match.
     */

    public static interface Result {

        /**
         * The absolute position in the buffer of the match.
         *
         * <p>
         * See discussion of Absolute and Relative methods above.
         * </p>
         */
        public long getIndex();

        /**
         * The number of errors in the match.
         */
        public int getErrors();

        /**
         * The score for the match.
         */
        public double getScore();
    }
}