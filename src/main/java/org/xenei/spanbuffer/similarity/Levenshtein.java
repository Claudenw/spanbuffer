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
package org.xenei.spanbuffer.similarity;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * A Levenshtein distance implementation.
 * <p>
 * This is the number of changes needed to change one buffer into another, where
 * each change is a single byte modification (deletion, insertion or
 * substitution).
 * </p>
 * <p>
 * This implementations is based on Apache Commons 3.5
 * StringUtils.getLevenshteinDistance
 * </p>
 */
public class Levenshtein {

    private static final Logger LOG = LoggerFactory.getLogger(Levenshtein.class);

    /**
     * Do not instantiate.
     */
    private Levenshtein() {
    }

    /**
     * <p>
     * Find the Levenshtein distance between two SpanBuffers.
     * </p>
     *
     * @param target  the target to get the distance from. Maximum length
     *                Integer.MAX_VALUE-1.
     * @param pattern the pattern to get the distance to. Maximum length
     *                Integer.MAX_VALUE-1.
     * @return result distance
     * @throws IllegalArgumentException if either buffer is longer than
     *                                  Integer.MAX_VALUE-1 or null
     */
    public static int distance(final SpanBuffer target, final SpanBuffer pattern) {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("pattern may not be null");
        }

        if ((target.getLength() > (Integer.MAX_VALUE - 1)) || (pattern.getLength() > (Integer.MAX_VALUE - 1))) {
            throw new IllegalArgumentException("buffers are too long");
        }
        SpanBuffer myTarget = target;
        SpanBuffer myPattern = pattern;
        /*
         * The difference between this impl. and the previous is that, rather than
         * creating and retaining a matrix of size s.length() + 1 by t.length() + 1, we
         * maintain two single-dimensional arrays of length s.length() + 1. The first,
         * d, is the 'current working' distance array that maintains the newest distance
         * cost counts as we iterate through the characters of String s. Each time we
         * increment the index of String t we are comparing, d is copied to p, the
         * second int[]. Doing so allows us to retain the previous cost counts as
         * required by the algorithm (taking the minimum of the cost count to the left,
         * up one, and diagonally up and to the left of the current cost count being
         * calculated). (Note that the arrays aren't really copied anymore, just
         * switched...this is clearly much better than cloning an array or doing a
         * System.arraycopy() each time through the outer loop.)
         *
         * Effectively, the difference between the two implementations is this one does
         * not cause an out of memory condition when calculating the LD over two very
         * large strings.
         */

        int myTargetLen = (int) myTarget.getLength(); // length of myTarget
        int myPatternLen = (int) myPattern.getLength(); // length of myPattern

        if (myTargetLen == 0) {
            return myPatternLen;
        } else if (myPatternLen == 0) {
            return myTargetLen;
        }

        if (myTargetLen > myPatternLen) {
            // swap the input strings to consume less memory
            final SpanBuffer tmp = myTarget;
            myTarget = myPattern;
            myPattern = tmp;
            myTargetLen = myPatternLen;
            myPatternLen = (int) myPattern.getLength();
        }

        int[] prevCostAry = new int[myTargetLen + 1]; // 'previous' cost array, horizontally
        int[] costAry = new int[myTargetLen + 1]; // cost array, horizontally
        int[] tmp; // placeholder to assist in swapping p and d

        // indexes into strings s and t
        int myTargetPos; // iterates through myTarget
        int myPatternPos; // iterates through myPattern

        byte myPatternPrev; // jth byte of t

        int cost; // cost

        for (myTargetPos = 0; myTargetPos <= myTargetLen; myTargetPos++) {
            prevCostAry[myTargetPos] = myTargetPos;
        }

        try {
            for (myPatternPos = 1; myPatternPos <= myPatternLen; myPatternPos++) {
                myPatternPrev = myPattern.read((myPattern.getOffset() + myPatternPos) - 1);
                costAry[0] = myPatternPos;

                for (myTargetPos = 1; myTargetPos <= myTargetLen; myTargetPos++) {
                    cost = myTarget.read((myTarget.getOffset() + myTargetPos) - 1) == myPatternPrev ? 0 : 1;
                    // minimum of cell to the left+1, to the top+1, diagonally
                    // left and up +cost
                    costAry[myTargetPos] = Math.min(
                            Math.min(costAry[myTargetPos - 1] + 1, prevCostAry[myTargetPos] + 1),
                            prevCostAry[myTargetPos - 1] + cost);
                }

                // copy current distance counts to 'previous row' distance
                // counts
                tmp = prevCostAry;
                prevCostAry = costAry;
                costAry = tmp;
            }

            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return prevCostAry[myTargetLen];
        } catch (final IOException ex) {
            LOG.warn("Error while reading data", ex);
            return -1;
        }
    }

    /**
     * Find the Levenshtein distance between two buffers if it's less than or equal
     * to a given threshold.
     *
     * @param target    the target to get the distance from. Maximum length
     *                  Integer.MAX_VALUE-1.
     * @param pattern   the pattern to get the distance to. Maximum length
     *                  Integer.MAX_VALUE-1.
     * @param threshold the target threshold, must not be negative
     * @return result distance, or {@code -1} if the distance would be greater than
     *         the threshold
     * @throws IllegalArgumentException if negative threshold
     */
    public static int distance(final SpanBuffer target, final SpanBuffer pattern, final int threshold) {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("pattern may not be null");
        }
        if (threshold < 0) {
            throw new IllegalArgumentException("Threshold must not be negative");
        }

        SpanBuffer myTarget = target;
        SpanBuffer myPattern = pattern;

        if ((myTarget.getLength() > (Integer.MAX_VALUE - 1)) || (myPattern.getLength() > (Integer.MAX_VALUE - 1))) {
            throw new IllegalArgumentException("Buffers are too long");
        }
        /*
         * This implementation only computes the distance if it's less than or equal to
         * the threshold value, returning -1 if it's greater. The advantage is
         * performance: unbounded distance is O(nm), but a bound of k allows us to
         * reduce it to O(km) time by only computing a diagonal stripe of width 2k + 1
         * of the cost table. It is also possible to use this to compute the unbounded
         * Levenshtein distance by starting the threshold at 1 and doubling each time
         * until the distance is found; this is O(dm), where d is the distance.
         *
         * One subtlety comes from needing to ignore entries on the border of our stripe
         * eg. p[] = |#|#|#|* d[] = *|#|#|#| We must ignore the entry to the left of the
         * leftmost member We must ignore the entry above the rightmost member
         *
         * Another subtlety comes from our stripe running off the matrix if the strings
         * aren't of the same size. Since string s is always swapped to be the shorter
         * of the two, the stripe will always run off to the upper right instead of the
         * lower left of the matrix.
         *
         * As a concrete example, suppose s is of length 5, t is of length 7, and our
         * threshold is 1. In this case we're going to walk a stripe of length 3. The
         * matrix would look like so:
         *
         * 1 2 3 4 5 1 |#|#| | | | 2 |#|#|#| | | 3 | |#|#|#| | 4 | | |#|#|#| 5 | | |
         * |#|#| 6 | | | | |#| 7 | | | | | |
         *
         * Note how the stripe leads off the table as there is no possible way to turn a
         * string of length 5 into one of length 7 in edit distance of 1.
         *
         * Additionally, this implementation decreases memory usage by using two
         * single-dimensional arrays and swapping them back and forth instead of
         * allocating an entire n by m matrix. This requires a few minor changes, such
         * as immediately returning when it's detected that the stripe has run off the
         * matrix and initially filling the arrays with large values so that entries we
         * don't compute are ignored.
         *
         * See Algorithms on Strings, Trees and Sequences by Dan Gusfield for some
         * discussion.
         */

        int myTargetLen = (int) myTarget.getLength(); // length of myTarget
        int myPatternLen = (int) myPattern.getLength(); // length of myPattern

        // if one string is empty, the edit distance is necessarily the length
        // of the other
        if (myTargetLen == 0) {
            return myPatternLen <= threshold ? myPatternLen : -1;
        } else if (myPatternLen == 0) {
            return myTargetLen <= threshold ? myTargetLen : -1;
        }

        if (myTargetLen > myPatternLen) {
            // swap the two strings to consume less memory
            myTarget = pattern;
            myPattern = target;
            myTargetLen = (int) myTarget.getLength(); // length of myTarget
            myPatternLen = (int) myPattern.getLength(); // length of myPattern
        }

        int[] prevCostAry = new int[myTargetLen + 1]; // 'previous' cost array, horizontally
        int[] costAry = new int[myTargetLen + 1]; // cost array, horizontally
        int[] tmp; // placeholder to assist in swapping p and d

        // fill in starting table values
        final int boundary = Math.min(myTargetLen, threshold) + 1;
        for (int i = 0; i < boundary; i++) {
            prevCostAry[i] = i;
        }
        // these fills ensure that the value above the rightmost entry of our
        // stripe will be ignored in following loop iterations
        Arrays.fill(prevCostAry, boundary, prevCostAry.length, Integer.MAX_VALUE);
        Arrays.fill(costAry, Integer.MAX_VALUE);

        try {
            // iterates through t
            for (int j = 1; j <= myPatternLen; j++) {
                final byte myPatternPrev = myPattern.read((myPattern.getOffset() + j) - 1); // jth
                // character
                // of
                // t
                costAry[0] = j;

                // compute stripe indices, constrain to array size
                final int min = Math.max(1, j - threshold);
                final int max = (j > (Integer.MAX_VALUE - threshold)) ? myTargetLen
                        : Math.min(myTargetLen, j + threshold);

                // the stripe may lead off of the table if s and t are of
                // different sizes
                if (min > max) {
                    return -1;
                }

                // ignore entry left of leftmost
                if (min > 1) {
                    costAry[min - 1] = Integer.MAX_VALUE;
                }

                // iterates through [min, max] in s
                for (int i = min; i <= max; i++) {
                    if (myTarget.read((myTarget.getOffset() + i) - 1) == myPatternPrev) {
                        // diagonally left and up
                        costAry[i] = prevCostAry[i - 1];
                    } else {
                        // 1 + minimum of cell to the left, to the top,
                        // diagonally left and up
                        costAry[i] = 1 + Math.min(Math.min(costAry[i - 1], prevCostAry[i]), prevCostAry[i - 1]);
                    }
                }

                // copy current distance counts to 'previous row' distance
                // counts
                tmp = prevCostAry;
                prevCostAry = costAry;
                costAry = tmp;
            }

            // if p[n] is greater than the threshold, there's no guarantee on it
            // being the correct
            // distance
            if (prevCostAry[myTargetLen] <= threshold) {
                return prevCostAry[myTargetLen];
            }
            return -1;
        } catch (final IOException ex) {
            LOG.warn("Error while reading data", ex);
            return -1;
        }
    }

}
