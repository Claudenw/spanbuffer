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
import java.io.IOException;

import org.junit.Test;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;

public class BitapTest {

	Bitap bitap = new Bitap();

	@Test
	public void bygFindExactMatchInMiddle() throws IOException {
		SpanBuffer haystack = Factory.wrap("TGATGCATTCGTAGATGC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Bitap.Result result = bitap.execute(haystack, needle);
		assertEquals(6, result.getAbsIndex());
	}

	@Test
	public void bygFindExactMatchAtStart() throws IOException {
		SpanBuffer haystack = Factory.wrap("ATTCGATGCATCAGTAGATGC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Bitap.Result result = bitap.execute(haystack, needle);
		assertEquals(0, result.getAbsIndex());

	}

	@Test
	public void bygFindExactMatchAtEnd() throws IOException {
		SpanBuffer haystack = Factory.wrap("GATGCATCAGTAGATGCATTC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Bitap.Result result = bitap.execute(haystack, needle);
		assertEquals(17, result.getAbsIndex());

	}

	@Test
	public void bygFindOverlappingExactMatches() throws IOException {
//			String haystack = "TGATGCATTATTAGTAGATGC";
//			String needle = "ATTA";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.baezaYatesGonnet(haystack);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(6);
//			test.add(9);
//			assertEquals(test, pos);
		SpanBuffer haystack = Factory.wrap("TGATGCATTATTAGTAGATGC");
		SpanBuffer needle = Factory.wrap("ATTA");
		Bitap.Result result = bitap.execute(haystack, needle);
		assertEquals(6, result.getAbsIndex());
		result = bitap.execute(haystack.sliceAt(result.getAbsIndex() + 1), needle);
		assertEquals(9, result.getAbsIndex());
	}

//		@Test
//		public void wuFindExactMatchInMiddle() {
//			String haystack = "TGATGCATTCGTAGATGC";
//			String needle = "ATTC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 0);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(6);
//			assertEquals(test, pos);		
//			assertTrue(bitap.within(haystack, 0));
//		}
//		
//		@Test
//		public void wuFindExactMatchAtStart() {
//			String haystack = "ATTCGATGCATCAGTAGATGC";
//			String needle = "ATTC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 0);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(0);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 0));
//		}
//		
//		@Test
//		public void wuFindExactMatchAtEnd() {
//			String haystack = "GATGCATCAGTAGATGCATTC";
//			String needle = "ATTC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 0);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(17);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 0));
//		}
//		
//		@Test
//		public void wuFindOverlappingExactMatches() {
//			String haystack = "TGATGCATTATTAGTAGATGC";
//			String needle = "ATTA";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 0);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(6);
//			test.add(9);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 0));
//		}
//		
//		@Test
//		public void wuFindMatchWithOneInternalDeletion() {
//			String haystack = "TGATGATTATTAGTAGATGC";
//			String needle = "ATGCATTAT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(2);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//		
//		@Test
//		public void wuFindMatchesWithOneInternalDeletion() {
//			String haystack = "TGATGTTAATCTAGGGCGTAATGATTGTTAGATTAGATTAGTAGATGC";
//			String needle = "GTTAGATCTAG";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(4);
//			test.add(26);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//		
//		@Test
//		public void wuFindMatchWithOneDeletionAtStart() {
//			String haystack = "GATGTTAATCTAGGGCGTAATGATTGTTAGATTAGATTAGTAGATGC";
//			String needle = "TGATGTTAATC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(0);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//
//		@Test
//		public void wuFindMatchWithTwoDeletionsAtStart() {
//			String haystack = "ATGTTAATCTAGGGCGTAATGATTGTTAGATTAGATTAGTAGATGC";
//			String needle = "TGATGTTAATC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 2);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(0);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 2));
//		}
//
//		@Test
//		public void wuFindMatchWithOneDeletionAtEnd() {
//			String haystack = "GATGTTAATCTAGGGCGTAATGATTGTTAGATTAGATTAGTAGATG";
//			String needle = "TTAGTAGATGC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(36);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//
//		@Test
//		public void wuFindMatchWithTwoDeletionsAtEnd() {
//			String haystack = "GATGTTAATCTAGGGCGTAATGATTGTTAGATTAGATTAGTAGAT";
//			String needle = "TTAGTAGATGC";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 2);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(36);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 2));
//		}
//
//		@Test
//		public void wuFindMatchWithOneInternalSubstitution() {
//			String haystack = "TGATCATTATTAGTAGATGC";
//			String needle = "ATGCATTAT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(2);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//		
//		@Test
//		public void wuFindMatchWithTwoInternalSubstitutions() {
//			String haystack = "TGATCTTTATTAGTAGATGC";
//			String needle = "ATGCATTAT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 2);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(2);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 2));
//		}
//		
//		@Test
//		public void wuFindMatchWithOneInternalInsertion() {
//			String haystack = "GATGTTAATCTAGGTGCGTAATGATTGTTAGATTAGATTAGTAGATG";
//			String needle = "AGGGCGTAATGATTGT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 1);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(11);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 1));
//		}
//		
//		@Test
//		public void wuFindMatchWithTwoInternalInsertions() {
//			String haystack = "GATGTTAATCTAGGTGCGTCAATGATTGTTAGATTAGATTAGTAGATG";
//			String needle = "AGGGCGTAATGATTGT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 2);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(11);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 2));
//		}
//		
//		@Test
//		public void wuFindMatchWithOneLongInternalInsertion() {
//			String haystack = "GATGTTAATCTAGGGCGTCCCAATGATTGTTAGATTAGATTAGTAGATG";
//			String needle = "AGGGCGTAATGATTGT";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 3);
//			List<Integer> test = new ArrayList<Integer>();
//			test.add(11);
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 3));
//		}
//		
//		@Test
//		public void wuDemonstrateAmbiguity() {
//			String haystack = "GGGGGGGGGGGGGAAAAAGGGGGGGGGGGGGGGG";
//			String needle = "AAAAA";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> pos = bitap.wuManber(haystack, 3);
//			List<Integer> test = new ArrayList<Integer>();
//			for (int i = 10; i <= 16; i++) {
//				test.add(i);
//			}
//			assertEquals(test, pos);
//			assertTrue(bitap.within(haystack, 3));
//		}
//		
//		@Test
//		public void wuCheckSequentialCase() {
//			String haystack = "GAGATGGATGACAACTTATACGGCCCCTACTTTTGACTTGCCCTCCACTTCATCCCGACAACTGGGCTTACTCGTGGGGTGACTTGTCATGTCTTCCGATCTTGTCTTGATTAGAAG";
//			String needle = "ACAACTGGGTCGTAGTCTTGGG";
//			Bitap bitap = new Bitap(needle, alphabet);
//			List<Integer> test = new ArrayList<Integer>();
//			
//			List<Integer> pos = bitap.wuManber(haystack, 0);
//			assertEquals(test, pos);
//			
//			pos = bitap.wuManber(haystack, 1);
//			assertEquals(test, pos);
//			
//			pos = bitap.wuManber(haystack, 2);
//			assertEquals(test, pos);
//			
//			pos = bitap.wuManber(haystack, 3);
//			assertEquals(test, pos);
//			
//			pos = bitap.wuManber(haystack, 4);
//			test.add(57);
//			assertEquals(test, pos);
//		}

}
