package org.xenei.spanbuffer.similarity;

import java.io.IOException;
import java.util.Arrays;

import org.xenei.spanbuffer.NoMatchException;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.Walker;

public class Bitap {
	public static class Config {
		
		/**
		 * The default match distance used by the match function.
		 */
		public static final int DEFAULT_MATCH_DISTANCE = 1000;
		/**
		 * The default match threshold used by the match function.
		 */
		public static final double DEFAULT_MATCH_THRESHOLD = 0.5d;
	
	
		private final  int distance;
		private final double threshold;
		
		public Config() {
			distance = DEFAULT_MATCH_DISTANCE;
			threshold = DEFAULT_MATCH_THRESHOLD;
		}
		
		public Config( final int distance, final double threshold)
		{
			this.distance = distance;
			this.threshold = threshold;
		}		
		
		/**
		 * Get the threshold for this matcher.
		 * @return the match threshold.
		 */
		public double getThreshold() {
			return threshold;
		}
		
		
		/**
		 * Get the current distance for for bitap match.
		 * @return the match distance.
		 */
		public int getDistance()
		{
			return distance;
		}
		
	}

	/**
	 * The result of a match call.
	 */
	public static class Result  {
	
		/**
		 * The absolute position in the buffer of the match.
		 */
		private long absIndex;

		/**
		 * The relative position in the buffer of the match.
		 */
		private long relIndex;

		/**
		 * The number of errors in the match.
		 */
		private int errors;
		/**
		 * The score for the match.
		 */
		private double score;
		
		/**
		 * The configuration used for the match.
		 */
		private final Bitap.Config config;
		
		/**
		 * Create a default result.
		 */
		public Result()
		{
			this( new Config() );
		}
		
		/**
		 * Create a result with the specified match positions.
		 * <p>
		 * See discussion of Absolute and Relative methods in SpanBuffer.
		 * </p>
		 * @See SpanBuffer
		 * 
		 * @param absStart the Absolute location
		 * @param relStart the Relative location.
		 */
		public Result(long absStart, long relStart )
		{
			this( new Config(), absStart, relStart );
		}
		
		/**
		 * Create a result with the specified configuration.
		 * @param config the bitap configuration.
		 */
		public Result( Bitap.Config config )
		{
			this(config,0,0);
		}
		
		/**
		 * Create a result with specified configuration and positions.
		 * <p>
		 * See discussion of Absolute and Relative methods in SpanBuffer.
		 * </p>
		 * @See SpanBuffer
		 * @param config the bitap config
		 * @param absStart the Absolute location
		 * @param relStart the Relative location.
		 */
		public Result( Bitap.Config config, long absStart, long relStart )
		{
			this.config = config;
			this.absIndex = absStart;
			this.relIndex = relStart;
		}
	
		/**
		 * The absolute position in the buffer of the match.
		 *
		 * <p>
		 * See discussion of Absolute and Relative methods in SpanBuffer.
		 * </p>
		 * @See SpanBuffer
		 */
		public final long getAbsIndex() {
			return absIndex;
		}
	
		/**
		 * The relative position in the buffer of the match.
		 *
		 * <p>
		 * See discussion of Absolute and Relative methods in SpanBuffer.
		 * </p>
		 * @See SpanBuffer
		 */
		public final long getRelIndex() {
			return relIndex;
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
	
		/**
		 * Get the configuration used to generate this result.
		 * @return the configuration.
		 */
		public final Config getConfig() {
			return config;
		}
	}

	private Bitap.Config config;
	
	/**
	 * The maximum size of a pattern for a BITAP match.
	 */
	public static final int MAX_BITAP = Integer.SIZE;
	
	/**
	 * Constructor with a default configuration.
	 */
	public Bitap() {
		this( new Config() );
	}
	
	/**
	 * Constructor with the specified configuration.
	 * @param config the bitap configuration that was used.
	 */
	public Bitap(Bitap.Config config)
	{ 
		this.config = config;
	}
	
	/**
	 * Set the configuration.
	 * @param config The configuration to use.
	 * @return this for chaining.
	 */
	public Bitap setConfig( Bitap.Config config )
	{
		this.config = config;
		return this;
	}
	
	/**
	 * Calculate the bitap distance. The tolerance is the number of errors
	 * acceptable in the matched string.
	 *
	 * <p>
	 * The resulting position (in the Result object) is relative.
	 * </p>
	 * <p>
 	 * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
	 * </p>
	 *
	 * @param pattern The pattern to match
	 * @return The result object representing match or null if no match was found.
	 * @throws IOException on error
	 */
	public Bitap.Result execute(final SpanBuffer target, final SpanBuffer pattern) throws IOException {
		return execute(target, pattern, target.getOffset());
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
	 * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
	 * </p>
	 *
	 * @param pattern The pattern to match
	 * @param loc     The absolute location to start the pattern match from.
	 * @return The result object representing match or null if no match was found.
	 * @throws IOException on error
	 */
	public Bitap.Result execute(final SpanBuffer target, final SpanBuffer pattern, final long loc) throws IOException {
		if (pattern.getLength() > MAX_BITAP) {
			throw new IllegalArgumentException(
					String.format("bitap argument too long, found %s bytes should be no more than %s",
							pattern.getLength(), MAX_BITAP));
		}

		final int patternLen = (int) pattern.getLength();
		final int targetLen = (int) target.getLength();
		// Initialize the alphabet.
		final int[] s = getByteMapSmall(pattern);

		// Highest score beyond which we give up.
		double scoreThreshold = this.config.threshold;
		final Bitap.Result retval = new Result( this.config);
		retval.absIndex = -1;
		retval.relIndex = target.makeRelative(-1);

		// Is there a nearby exact match? (speedup)
		// int best_loc;

		try {
			retval.absIndex = (int) target.positionOf(pattern, loc);
			retval.relIndex = target.makeRelative( retval.absIndex );
			retval.score = bitapScore(0, retval.absIndex, loc, pattern);
			scoreThreshold = Math.min(retval.score, scoreThreshold);
			// What about in the other direction? (speedup)
			final int idx = (int) target.lastPositionOf(pattern, loc + patternLen);
			final double score = bitapScore(0, idx, loc, pattern);
			if (score < scoreThreshold) {
				retval.absIndex = idx;
				retval.relIndex = target.makeRelative( retval.absIndex );
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
						retval.relIndex =  j-1 ;
						retval.absIndex = target.makeAbsolute( retval.relIndex );
						retval.score = score;
						retval.errors = errorCount;

						if (retval.getAbsIndex() > loc) {
							// When passing loc, don't exceed our current
							// distance from loc.
							start = Math.max(1, (2 * loc) - (int) retval.getAbsIndex());
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
		return retval.absIndex > -1 ? retval : null;
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
		if (this.config.distance == 0) {
			// Dodge divide by zero error.
			return matchPos == 0 ? accuracy : 1.0;
		}
		final double result = accuracy + (proximity / (double) this.config.distance);
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
	 * Convert a byte to an int.
	 * @param b the byte to convert.
	 * @return the integer value for the byte bit pattern.
	 */
	private int cb(final byte b) {
		return (b & 0xFF);
	}

}