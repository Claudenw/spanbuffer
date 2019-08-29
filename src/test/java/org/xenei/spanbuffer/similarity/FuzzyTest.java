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

import org.junit.Test;
import org.xenei.spanbuffer.Factory;

public class FuzzyTest {

	@Test
	public void testGetFuzzyScore() {
		assertEquals(0, Fuzzy.score(Factory.EMPTY, Factory.EMPTY));
		assertEquals(0, Fuzzy.score(Factory.wrap("Workshop"), Factory.wrap("b")));
		assertEquals(1, Fuzzy.score(Factory.wrap("Room"), Factory.wrap("o")));
		assertEquals(1, Fuzzy.score(Factory.wrap("Workshop"), Factory.wrap("W")));
		assertEquals(2, Fuzzy.score(Factory.wrap("Workshop"), Factory.wrap("Ws")));
		assertEquals(4, Fuzzy.score(Factory.wrap("Workshop"), Factory.wrap("Wo")));
		assertEquals(3, Fuzzy.score(Factory.wrap("Apache Software Foundation"), Factory.wrap("ASF")));
	}

	@Test
	public void testNullArguments() {
		try {
			Fuzzy.score(Factory.EMPTY, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}

		try {
			Fuzzy.score(null, Factory.EMPTY);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}

	}

}
