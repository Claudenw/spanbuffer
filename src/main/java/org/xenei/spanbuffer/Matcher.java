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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default matcher implementation.
 */
public class Matcher  {
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

	
	private static final Logger LOG = LoggerFactory.getLogger(Matcher.class);

	private final SpanBuffer target;
	private int matchDistance;
	private double matchThreshold;

	/**
	 * Constructor.
	 * 
	 * @param target Span buffer to match against.
	 */
	public Matcher(final SpanBuffer target) {

		// Check for null inputs.
		if (target == null) {
			throw new IllegalArgumentException("Target may not be null");
		}

		this.target = target;

		matchThreshold = DEFAULT_MATCH_THRESHOLD;
		matchDistance = DEFAULT_MATCH_DISTANCE;
	}

	private int cb(final byte i) {
		return (i & 0xFF);
	}

	/**
	 * Set the threshold for a match. Default value is 0.5
	 *
	 * @param threshold the new threshold.
	 * @return the old threshold.
	 */
	public double setThreshold(final double threshold) {
		final double old = matchThreshold;
		matchThreshold = threshold;
		return old;
	}

	/**
	 * Set the distance for a match.
	 *
	 * @param distance The new distance value
	 * @return the old distance value
	 */
	public int setDistance(final int distance) {
		final int old = matchDistance;
		matchDistance = distance;
		return old;
	}

	/**
	 * Match a pattern starting at the specified relative position.
	 *
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param pattern  The pattern to match.
	 * @param position The relative position to start matching at
	 * @return The relative match position.
	 * @throws NoMatchException if match not found
	 * @throws IOException      ion IOerror
	 */
	public long matchFrom(final SpanBuffer pattern, final long loc) throws NoMatchException, IOException {
		// Check for null inputs.
		if (pattern == null) {
			throw new IllegalArgumentException("pattern may not be null");
		}

		// check for pattern to long
		if (pattern.getLength() > target.getLength()) {
			throw new NoMatchException();
		}

		// Perfect match at the perfect spot! (Includes case of null pattern)
		final SpanBuffer subTarget = target.cut(loc);
		if (subTarget.startsWith(pattern)) {
			return loc;
		}

		final Result result = bitap(pattern, loc);
		if (result == null) {
			throw new NoMatchException();
		}
		return result.getIndex();
	}

	/**
	 * Get a match result from an attempted match.
	 *
	 * @param pattern the pattern to match
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public Result match(final SpanBuffer pattern) throws NoMatchException, IOException {
		return match(pattern, 0);
	}

	/**
	 * Get a match result from an attempted match.
	 *
	 * @param pattern the pattern to match
	 * @param loc     the location to start the match from.
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public Result match(final SpanBuffer pattern, final long loc) throws NoMatchException, IOException {
		// Check for null inputs.
		if (pattern == null) {
			throw new IllegalArgumentException("pattern may not be null");
		}

		// Perfect match at the perfect spot! (Includes case of null pattern)
		final SpanBuffer subTarget = target.cut(loc);
		Result result = null;
		if (subTarget.startsWith(pattern)) {
			result = new Result();
			result.index = loc;
		} else {
			result = bitap(pattern, loc);
			if (result == null) {
				throw new NoMatchException();
			}
		}
		return result;
	}

	/**
	 * initialize the byte array.
	 *
	 * @param pattern The text to encode.
	 * @return byte locations bitmaps.
	 * @throws IOException throws IOException
	 */
	private int[] getByteMapSmall(final SpanBuffer pattern) throws IOException {
		final int[] retval = new int[256];
		Arrays.fill(retval, 0);
		final Walker walker = pattern.getWalker();
		while (walker.hasCurrent()) {
			// (1 << (pattern.length() - i - 1)
			retval[cb(walker.getByte())] |= (1 << (int) (walker.remaining() - 1));
			walker.next();
		}
		return retval;
	}

	
	/**
	 * <p>
	 * Find the Fuzzy Distance which indicates the similarity score between two
	 * buffers.
	 * </p>
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
	 * @param pattern the pattern to get the distance to.
	 * @return result score
	 */
	public int getFuzzyDistance(final SpanBuffer pattern) {
		// Check for null inputs.
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
			Matcher.LOG.warn("Error while reading data", ex);
			return -1;
		}
	}


	/**
	 * <p>
	 * Find the Levenshtein distance between two SpanBuffers.
	 * </p>
	 *
	 * <p>
	 * This is the number of changes needed to change one buffer into another, where
	 * each change is a single byte modification (deletion, insertion or
	 * substitution).
	 * </p>
	 * <p>
	 * This implementations is based on Apache Commons 3.5
	 * StringUtils.getLevenshteinDistance
	 * </p>
	 *
	 * @param pattern the pattern to get the distance to.
	 * @return result distance
	 * @throws IllegalArgumentException if either buffer is longer than
	 *                                  Integer.MAX_VALUE-1
	 */
	public int getLevenshteinDistance(final SpanBuffer pattern) {
		// Check for null inputs.
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
			Matcher.LOG.warn("Error while reading data", ex);
			return -1;
		}
	}

	/**
	 * Find the Levenshtein distance between two buffers if it's less than or equal
	 * to a given threshold.
	 *
	 * <p>
	 * This is the number of changes needed to change one buffer into another, where
	 * each change is a single byte modification (deletion, insertion or
	 * substitution).
	 * </p>
	 *
	 * <p>
	 * This implementations is based on Apache Commons 3.5
	 * StringUtils.getLevenshteinDistance
	 * </p>
	 *
	 * @param pattern   The pattern to get the distance to.
	 * @param threshold the target threshold, must not be negative
	 * @return result distance, or {@code -1} if the distance would be greater than
	 *         the threshold
	 * @throws IllegalArgumentException if negative threshold
	 */
	public int getLevenshteinDistance(final SpanBuffer pattern, final int threshold) {
		// Check for null inputs.
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
			Matcher.LOG.warn("Error while reading data", ex);
			return -1;
		}
	}

	/**
	 * Calculate the bitap distance. The tolerance is the number of errors
	 * acceptable in the matched string.
	 *
	 * <p>
	 * The resulting position (in the Result object) is relative.
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param pattern The pattern to match
	 * @return The result object representing match or null if no match was found.
	 * @throws IOException on error
	 */
	public Result bitap(final SpanBuffer pattern) throws IOException {
		return bitap(pattern, target.getOffset());
	}

	/**
	 * Calculate the bitap distance. The tolerance is the number of errors
	 * acceptable in the matched string.
	 *
	 * <p>
	 * The resulting position (in the Result object) is relative.
	 *
	 * </p>
	 * <p>
	 * See discussion of Absolute and Relative methods above.
	 * </p>
	 *
	 * @param pattern The pattern to match
	 * @param loc     The absolute location to start the pattern match from.
	 * @return The result object representing match or null if no match was found.
	 * @throws IOException on error
	 */
	public Result bitap(final SpanBuffer pattern, final long loc) throws IOException {
		if (pattern.getLength() > Matcher.MAX_BITAP) {
			throw new IllegalArgumentException(
					String.format("bitap argument too long, found %s bytes should be no more than %s",
							pattern.getLength(), Matcher.MAX_BITAP));
		}

		final int patternLen = (int) pattern.getLength();
		final int targetLen = (int) target.getLength();
		// Initialize the alphabet.
		final int[] s = getByteMapSmall(pattern);

		// Highest score beyond which we give up.
		double scoreThreshold = matchThreshold;
		final Result retval = new Result();
		retval.index = -1;

		// Is there a nearby exact match? (speedup)
		// int best_loc;

		try {
			retval.index = (int) target.positionOf(pattern, loc);
			retval.score = bitapScore(0, retval.index, loc, pattern);
			scoreThreshold = Math.min(retval.score, scoreThreshold);
			// What about in the other direction? (speedup)
			final int idx = (int) target.lastPositionOf(pattern, loc + patternLen);
			final double score = bitapScore(0, idx, loc, pattern);
			if (score < scoreThreshold) {
				retval.index = idx;
				retval.score = score;
				scoreThreshold = score;
			}
		} catch (final NoMatchException expected) {
			// do nothing
		}

		// Initialise the bit arrays.
		final int matchmask = 1 << (patternLen - 1);

		long binMin;
		long binMid;
		long binMax = patternLen + targetLen;
		// Empty initialization added to appease Java compiler.
		int[] lastRd = new int[0];
		for (int errorCount = 0; errorCount < patternLen; errorCount++) {
			/*
			 * Scan for the best match; each iteration allows for one more error. Run a
			 * binary search to determine how far from 'loc' we can stray at this error
			 * level.
			 */
			binMin = 0;
			binMid = binMax;
			while (binMin < binMid) {
				if (bitapScore(errorCount, loc + binMid, loc, pattern) <= scoreThreshold) {
					binMin = binMid;
				} else {
					binMax = binMid;
				}
				binMid = ((binMax - binMin) / 2) + binMin;
			}
			// Use the result from this iteration as the maximum for the next.
			binMax = binMid;
			long start = Math.max(1, (loc - binMid) + 1);
			final int finish = (int) Math.min(loc + binMid, targetLen) + patternLen;

			final int[] rd = new int[finish + 2];
			rd[finish + 1] = (1 << errorCount) - 1;
			for (int j = finish; j >= start; j--) {
				int charMatch;
				if ((targetLen <= (j - 1)) || (s[cb(target.readRelative(j - 1))] == 0)) {
					// Out of range.
					charMatch = 0;
				} else {
					charMatch = s[cb(target.readRelative(j - 1))];
				}
				if (errorCount == 0) {
					// First pass: exact match.
					rd[j] = ((rd[j + 1] << 1) | 1) & charMatch;
				} else {
					// Subsequent passes: fuzzy match.
					rd[j] = (((rd[j + 1] << 1) | 1) & charMatch) | (((lastRd[j + 1] | lastRd[j]) << 1) | 1)
							| lastRd[j + 1];
				}
				if ((rd[j] & matchmask) != 0) {
					final double score = bitapScore(errorCount, target.makeAbsolute(j - 1), loc, pattern);
					// This match will almost certainly be better than any
					// existing
					// match. But check anyway.
					if (score <= scoreThreshold) {
						// Told you so.
						scoreThreshold = score;
						retval.index = target.makeAbsolute(j - 1);
						retval.score = score;
						retval.errors = errorCount;

						if (retval.getIndex() > loc) {
							// When passing loc, don't exceed our current
							// distance from loc.
							start = Math.max(1, (2 * loc) - (int) retval.getIndex());
						} else {
							// Already passed loc, downhill from here on in.
							break;
						}
					}
				}
			}
			if (bitapScore(errorCount + 1, loc, loc, pattern) > scoreThreshold) {
				// No hope for a (better) match at greater error levels.
				break;
			}
			lastRd = rd;
		}
		return retval.index > -1 ? retval : null;
	}

	/**
	 * Compute and return the score for a match with errCnt errors and matchPos
	 * location.
	 *
	 * @param errCnt   Number of errors in match.
	 * @param matchPos Location of match.
	 * @param loc      Expected location of match.
	 * @param pattern  Pattern being sought.
	 * @return Overall score for match (0.0 = good, 1.0 = bad).
	 */
	private double bitapScore(final int errCnt, final long matchPos, final long loc, final SpanBuffer pattern) {
		final double accuracy = ((double) errCnt) / pattern.getLength();
		final long proximity = Math.abs(loc - matchPos);
		if (matchDistance == 0) {
			// Dodge divide by zero error.
			return matchPos == 0 ? accuracy : 1.0;
		}
		final double result = accuracy + (proximity / (double) matchDistance);
		return result;
	}

	public static class Result  {

		/**
		 * The absolute position in the buffer of the match.
		 *
		 * <p>
		 * See discussion of Absolute and Relative methods above.
		 * </p>
		 */
		private long index;
		/**
		 * The number of errors in the match.
		 */
		private int errors;
		/**
		 * The score for the match.
		 */
		private double score;

		/**
		 * The absolute position in the buffer of the match.
		 *
		 * <p>
		 * See discussion of Absolute and Relative methods above.
		 * </p>
		 */
		public final long getIndex() {
			return index;
		}

		/**
		 * The number of errors in the match.
		 */
		public final int getErrors() {
			return errors;
		}

		/**
		 * The score for the match.
		 */
		public final double getScore() {
			return score;
		}

	}

}