package org.xenei.spanbuffer.similarity;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.Walker;

public class Fuzzy {
	
	private static final Logger LOG = LoggerFactory.getLogger(Fuzzy.class);

	/**
	 * Don't instantiate.
	 */
	private Fuzzy() {		
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
	 * @param target the target to get the distance from.
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
