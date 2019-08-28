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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.similarity.Bitap;

/**
 * Matcher implementation for SpanBuffers.  Provides some common
 * matching functions.  Some of the code in this module is 
 * based on Apache Commons Lang 3.5
 * StringUtils.getLevenshteinDistance
 */
public class Matcher  {
	private static final Logger LOG = LoggerFactory.getLogger(Matcher.class);


//	/**
//	 * Match a pattern starting at the specified relative position.
//	 *
//	 * <p>
//	 * See discussion of Absolute and Relative methods in SpanBuffer javadoc.
//	 * </p>
//	 *
//	 * @param pattern  The pattern to match.
//	 * @param position The relative position to start matching at
//	 * @return The relative match position.
//	 * @throws NoMatchException if match not found
//	 * @throws IOException      ion IOerror
//	 */
//	public long matchFrom(final SpanBuffer target, final SpanBuffer pattern, final long loc) throws NoMatchException, IOException {
//		// Check for null inputs.
//		if (target == null) {
//			throw new IllegalArgumentException("target may not be null");
//		}
//		if (pattern == null) {
//			throw new IllegalArgumentException("pattern may not be null");
//		}
//
//		// check for pattern to long
//		if (pattern.getLength() > target.getLength()) {
//			throw new NoMatchException();
//		}
//
//		// Perfect match at the perfect spot! (Includes case of null pattern)
//		final SpanBuffer subTarget = target.cut(loc);
//		if (subTarget.startsWith(pattern)) {
//			return loc;
//		}
//		Bitap bitap = new Bitap();
//		final Bitap.Result result = bitap.execute(target, pattern, loc);
//		if (result == null) {
//			throw new NoMatchException();
//		}
//		return result.getIndex();
//	}

	/**
	 * Get a match result from an attempted match.
	 *
	 * @param target the target find the pattern in.
	 * @param pattern the pattern to match
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern) throws NoMatchException, IOException {
		return match(target, pattern, target.getOffset());
	}

	/**
	 * Get a match result starting from a previous match.
	 *
	 * @param target the target find the pattern in.
	 * @param pattern the pattern to match
	 * @param prevResult  A previous result.
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final Bitap.Result prevResult) throws NoMatchException, IOException {
		return match( target, pattern, prevResult.getIndex()+1, prevResult.getConfig());
	}

	/**
	 * Get a match result starting from a specific position.
	 *
	 * @param target the target find the pattern in.
	 * @param pattern the pattern to match
	 * @param loc     the location to start the match from.
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final long start) throws NoMatchException, IOException {
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
			result = new Bitap.Result(start);
		} else {
			Bitap bitap = new Bitap();
			result = bitap.execute(subTarget, pattern, subTarget.getOffset() );
			if (result == null) {
				throw new NoMatchException();
			}
		}
		return result;
	}

	/**
	 * Get a match result starting from a specific position.
	 *
	 * @param target the target find the pattern in.
	 * @param pattern the pattern to match
	 * @param start  the location to start the match from.
	 * @param config the Bitap.Config to use for the match.
	 * @return The match result.
	 * @throws NoMatchException if a match could not be established
	 * @throws IOException      on IO error.
	 */
	public static Bitap.Result match(final SpanBuffer target, final SpanBuffer pattern, final long start, Bitap.Config config) throws NoMatchException, IOException {
		// Check for null inputs.
		if (target == null) {
			throw new IllegalArgumentException("target may not be null");
		}
		if (pattern == null) {
			throw new IllegalArgumentException("pattern may not be null");
		}

		// Perfect match at the perfect spot! (Includes case of null pattern)
		final SpanBuffer subTarget = target.cut(start);
		Bitap.Result result = null;
		if (subTarget.startsWith(pattern)) {
			result = new Bitap.Result(config, start);
		} else {
			Bitap bitap = new Bitap( config );
			result = bitap.execute(subTarget, pattern, subTarget.getOffset() );

			if (result == null) {
				throw new NoMatchException();
			}
		}
		return result;
	}

}