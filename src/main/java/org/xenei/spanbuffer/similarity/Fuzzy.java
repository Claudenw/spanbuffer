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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.Walker;

/**
 * A fuzzy match implementation.
 *
 * <p>
 * This buffer matching algorithm is similar to the algorithms of editors such
 * as Sublime Text, TextMate, Atom and others. One point is given for every
 * matched byte. Subsequent matches yield two bonus points. A higher score
 * indicates a higher similarity.
 * </p>
 * <p>
 * This implementations is based on Apache Commons 3.5
 * StringUtils.getLevenshteinDistance
 * </p>
 *
 */
public class Fuzzy {

    private static final Logger LOG = LoggerFactory.getLogger(Fuzzy.class);

    /**
     * Don't instantiate.
     */
    private Fuzzy() {
    }

    /**
     * Find the Fuzzy Distance which indicates the similarity score between two
     * buffers.
     *
     * @param target  the target to get the distance from.
     * @param pattern the pattern to get the distance to.
     * @return result score
     */
    public static int score(final SpanBuffer target, final SpanBuffer pattern) {
        // Check for null inputs.
        if (target == null) {
            throw new IllegalArgumentException("target may not be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("pattern may not be null");
        }

        // the resulting score
        int score = 0;

        // index of the previously matched character in the term
        long previousMatchingCharacterIndex = target.getOffset() - 2;

        final Walker patternWalker = pattern.getWalker();
        final Walker targetWalker = target.getWalker();

        try {
            // for (int patternIndex = 0; patternIndex <
            // patternLowerCase.length(); patternIndex++) {
            while (patternWalker.hasCurrent()) {

                // final char patternChar =
                // patternLowerCase.charAt(patternIndex);
                final byte patternByte = patternWalker.getByte();
                boolean termCharacterMatchFound = false;

                while (targetWalker.hasCurrent() && !termCharacterMatchFound) {

                    final byte targetByte = targetWalker.getByte();

                    if (patternByte == targetByte) {
                        // simple character matches result in one point
                        score++;

                        // subsequent character matches further improve
                        // the score.
                        if ((previousMatchingCharacterIndex + 1) == targetWalker.getPos()) {
                            score += 2;
                        }

                        previousMatchingCharacterIndex = targetWalker.getPos();

                        // we can leave the nested loop. Every character in the
                        // query can match at most one character in the term.
                        termCharacterMatchFound = true;
                    }
                    targetWalker.next();
                }
                patternWalker.next();
            }

            return score;
        } catch (final IOException ex) {
            LOG.warn("Error while reading data", ex);
            return -1;
        }
    }

}
