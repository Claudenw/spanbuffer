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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public class LevenshteinTest {

//	@Test
//	public void testConstructorNull() {
//		try {
//			new Matcher(null);
//			fail("Should have thrown IllegalArgumentException");
//		} catch (IllegalArgumentException expected) {
//		}
//	}

	///////////////////// LEVENSHTEIN DISTANCE TESTS

	@Test
	public void testGetLevenshteinDistance_StringString() {
		assertEquals(0, Levenshtein.distance(Factory.EMPTY, Factory.EMPTY));
		assertEquals(1, Levenshtein.distance(Factory.EMPTY, Factory.wrap("a")));
		assertEquals(7, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.EMPTY));
		assertEquals(1, Levenshtein.distance(Factory.wrap("frog"), Factory.wrap("fog")));
		assertEquals(3, Levenshtein.distance(Factory.wrap("fly"), Factory.wrap("ant")));
		assertEquals(7, Levenshtein.distance(Factory.wrap("elephant"), Factory.wrap("hippo")));
		assertEquals(7, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("elephant")));
		assertEquals(8, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("zzzzzzzz")));
		assertEquals(8, Levenshtein.distance(Factory.wrap("zzzzzzzz"), Factory.wrap("hippo")));
		assertEquals(1, Levenshtein.distance(Factory.wrap("hello"), Factory.wrap("hallo")));
	}

	@Test
	public void testNullArguments() throws IOException {
		try {
			Levenshtein.distance(Factory.wrap("a"), null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
		try {
			Levenshtein.distance(null, Factory.wrap("a"));
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

	@Test
	public void testGetLevenshteinDistance_StringStringInt() {
		// empty strings

		assertEquals(0, Levenshtein.distance(Factory.EMPTY, Factory.EMPTY, 0));

		assertEquals(7, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.EMPTY, 8));
		assertEquals(7, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.EMPTY, 7));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.EMPTY, 6));

		// unequal strings, zero threshold
		assertEquals(-1, Levenshtein.distance(Factory.wrap("a"), Factory.wrap("b"), 0));

		assertEquals(-1, Levenshtein.distance(Factory.wrap("b"), Factory.wrap("a"), 0));

		// equal strings
		SpanBuffer a = Factory.wrap("aa");
		assertEquals(0, Levenshtein.distance(a, a, 0));
		assertEquals(0, Levenshtein.distance(a, a, 2));

		// same length
		a = Factory.wrap("aaa");
		SpanBuffer b = Factory.wrap("bbb");
		assertEquals(-1, Levenshtein.distance(a, b, 2));
		assertEquals(3, Levenshtein.distance(a, b, 3));

		// big stripe
		assertEquals(6, Levenshtein.distance(Factory.wrap("aaaaaa"), Factory.wrap("b"), 10));

		// distance less than threshold
		assertEquals(7, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.wrap("b"), 8));

		assertEquals(3, Levenshtein.distance(Factory.wrap("a"), Factory.wrap("bbb"), 4));

		// distance equal to threshold
		assertEquals(7, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.wrap("b"), 7));
		assertEquals(3, Levenshtein.distance(Factory.wrap("a"), Factory.wrap("bbb"), 3));

		// distance greater than threshold
		assertEquals(-1, Levenshtein.distance(Factory.wrap("a"), Factory.wrap("bbb"), 2));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("bbb"), Factory.wrap("a"), 2));

		assertEquals(-1, Levenshtein.distance(Factory.wrap("aaapppp"), Factory.wrap("b"), 6));

		// stripe runs off array, strings not similar

		assertEquals(-1, Levenshtein.distance(Factory.wrap("a"), Factory.wrap("bbb"), 1));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("bbb"), Factory.wrap("a"), 1));

		// stripe runs off array, strings are similar
		assertEquals(-1, Levenshtein.distance(Factory.wrap("12345"), Factory.wrap("1234567"), 1));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("1234567"), Factory.wrap("12345"), 1));

		// old getLevenshteinDistance test cases

		assertEquals(1, Levenshtein.distance(Factory.wrap("frog"), Factory.wrap("fog"), 1));

		assertEquals(3, Levenshtein.distance(Factory.wrap("fly"), Factory.wrap("ant"), 3));

		assertEquals(7, Levenshtein.distance(Factory.wrap("elephant"), Factory.wrap("hippo"), 7));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("elephant"), Factory.wrap("hippo"), 6));

		assertEquals(7, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("elephant"), 7));
		assertEquals(-1, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("elephant"), 6));
		assertEquals(8, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("zzzzzzzz"), 8));

		assertEquals(8, Levenshtein.distance(Factory.wrap("zzzzzzzz"), Factory.wrap("hippo"), 8));

		assertEquals(1, Levenshtein.distance(Factory.wrap("hello"), Factory.wrap("hallo"), 1));

		assertEquals(1, Levenshtein.distance(Factory.wrap("frog"), Factory.wrap("fog"), Integer.MAX_VALUE));

		assertEquals(3, Levenshtein.distance(Factory.wrap("fly"), Factory.wrap("ant"), Integer.MAX_VALUE));

		assertEquals(7, Levenshtein.distance(Factory.wrap("elephant"), Factory.wrap("hippo"), Integer.MAX_VALUE));

		assertEquals(7, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("elephant"), Integer.MAX_VALUE));
		assertEquals(8, Levenshtein.distance(Factory.wrap("hippo"), Factory.wrap("zzzzzzzz"), Integer.MAX_VALUE));

		assertEquals(8, Levenshtein.distance(Factory.wrap("zzzzzzzz"), Factory.wrap("hippo"), Integer.MAX_VALUE));

		assertEquals(1, Levenshtein.distance(Factory.wrap("hello"), Factory.wrap("hallo"), Integer.MAX_VALUE));

	}

	@Test
	public void testNegativeThreshold() {
		try {
			Levenshtein.distance(Factory.wrap("hello"), Factory.EMPTY, -1);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

}
