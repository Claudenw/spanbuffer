package org.xenei.spanbuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

public class MatcherTest {

	@Test
	public void testConstructorNull() {
		try {
			new Matcher(null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

	///////////////////// LEVENSHTEIN DISTANCE TESTS

	@Test
	public void testGetLevenshteinDistance_StringString() {
		assertEquals(0, Factory.EMPTY.getMatcher().getLevenshteinDistance(Factory.EMPTY));
		assertEquals(1, Factory.EMPTY.getMatcher().getLevenshteinDistance(Factory.wrap("a")));
		assertEquals(7, Factory.wrap("aaapppp").getMatcher().getLevenshteinDistance(Factory.EMPTY));
		assertEquals(1, Factory.wrap("frog").getMatcher().getLevenshteinDistance(Factory.wrap("fog")));
		assertEquals(3, Factory.wrap("fly").getMatcher().getLevenshteinDistance(Factory.wrap("ant")));
		assertEquals(7, Factory.wrap("elephant").getMatcher().getLevenshteinDistance(Factory.wrap("hippo")));
		assertEquals(7, Factory.wrap("hippo").getMatcher().getLevenshteinDistance(Factory.wrap("elephant")));
		assertEquals(8, Factory.wrap("hippo").getMatcher().getLevenshteinDistance(Factory.wrap("zzzzzzzz")));
		assertEquals(8, Factory.wrap("zzzzzzzz").getMatcher().getLevenshteinDistance(Factory.wrap("hippo")));
		assertEquals(1, Factory.wrap("hello").getMatcher().getLevenshteinDistance(Factory.wrap("hallo")));
	}

	@Test
	public void testGetLevenshteinDistance_NullString() throws NoMatchException, IOException {
		try {
			Factory.wrap("a").getMatcher().match(null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

	@Test
	public void testGetLevenshteinDistance_StringStringInt() {
		// empty strings
		Matcher matcher = Factory.EMPTY.getMatcher();
		assertEquals(0, matcher.getLevenshteinDistance(Factory.EMPTY, 0));

		matcher = Factory.wrap("aaapppp").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.EMPTY, 8));
		assertEquals(7, matcher.getLevenshteinDistance(Factory.EMPTY, 7));
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.EMPTY, 6));

		// unequal strings, zero threshold
		matcher = Factory.wrap("a").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("b"), 0));

		matcher = Factory.wrap("b").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("a"), 0));

		// equal strings
		SpanBuffer a = Factory.wrap("aa");
		matcher = a.getMatcher();
		assertEquals(0, matcher.getLevenshteinDistance(a, 0));
		assertEquals(0, matcher.getLevenshteinDistance(a, 2));

		// same length
		a = Factory.wrap("aaa");
		SpanBuffer b = Factory.wrap("bbb");
		matcher = a.getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(b, 2));
		assertEquals(3, matcher.getLevenshteinDistance(b, 3));

		// big stripe
		matcher = Factory.wrap("aaaaaa").getMatcher();
		assertEquals(6, matcher.getLevenshteinDistance(Factory.wrap("b"), 10));

		// distance less than threshold
		matcher = Factory.wrap("aaapppp").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("b"), 8));

		matcher = Factory.wrap("a").getMatcher();
		assertEquals(3, matcher.getLevenshteinDistance(Factory.wrap("bbb"), 4));

		// distance equal to threshold
		matcher = Factory.wrap("aaapppp").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("b"), 7));
		matcher = Factory.wrap("a").getMatcher();
		assertEquals(3, matcher.getLevenshteinDistance(Factory.wrap("bbb"), 3));

		// distance greater than threshold
		matcher = Factory.wrap("a").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("bbb"), 2));
		matcher = Factory.wrap("bbb").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("a"), 2));

		matcher = Factory.wrap("aaapppp").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("b"), 6));

		// stripe runs off array, strings not similar

		matcher = Factory.wrap("a").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("bbb"), 1));
		matcher = Factory.wrap("bbb").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("a"), 1));

		// stripe runs off array, strings are similar
		matcher = Factory.wrap("12345").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("1234567"), 1));
		matcher = Factory.wrap("1234567").getMatcher();
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("12345"), 1));

		// old getLevenshteinDistance test cases

		matcher = Factory.wrap("frog").getMatcher();
		assertEquals(1, matcher.getLevenshteinDistance(Factory.wrap("fog"), 1));

		matcher = Factory.wrap("fly").getMatcher();
		assertEquals(3, matcher.getLevenshteinDistance(Factory.wrap("ant"), 3));

		matcher = Factory.wrap("elephant").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("hippo"), 7));
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("hippo"), 6));

		matcher = Factory.wrap("hippo").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("elephant"), 7));
		assertEquals(-1, matcher.getLevenshteinDistance(Factory.wrap("elephant"), 6));
		assertEquals(8, matcher.getLevenshteinDistance(Factory.wrap("zzzzzzzz"), 8));

		matcher = Factory.wrap("zzzzzzzz").getMatcher();
		assertEquals(8, matcher.getLevenshteinDistance(Factory.wrap("hippo"), 8));

		matcher = Factory.wrap("hello").getMatcher();
		assertEquals(1, matcher.getLevenshteinDistance(Factory.wrap("hallo"), 1));

		matcher = Factory.wrap("frog").getMatcher();
		assertEquals(1, matcher.getLevenshteinDistance(Factory.wrap("fog"), Integer.MAX_VALUE));

		matcher = Factory.wrap("fly").getMatcher();
		assertEquals(3, matcher.getLevenshteinDistance(Factory.wrap("ant"), Integer.MAX_VALUE));

		matcher = Factory.wrap("elephant").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("hippo"), Integer.MAX_VALUE));

		matcher = Factory.wrap("hippo").getMatcher();
		assertEquals(7, matcher.getLevenshteinDistance(Factory.wrap("elephant"), Integer.MAX_VALUE));
		assertEquals(8, matcher.getLevenshteinDistance(Factory.wrap("zzzzzzzz"), Integer.MAX_VALUE));

		matcher = Factory.wrap("zzzzzzzz").getMatcher();
		assertEquals(8, matcher.getLevenshteinDistance(Factory.wrap("hippo"), Integer.MAX_VALUE));

		matcher = Factory.wrap("hello").getMatcher();
		assertEquals(1, matcher.getLevenshteinDistance(Factory.wrap("hallo"), Integer.MAX_VALUE));

	}

	@Test
	public void testNegativeThreshold() {
		try {
			Factory.wrap("hello").getMatcher().getLevenshteinDistance(Factory.EMPTY, -1);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

	///////////////// FUZZY TESTS

	@Test
	public void testGetFuzzyScore() {
		assertEquals(0, Factory.EMPTY.getMatcher().getFuzzyDistance(Factory.EMPTY));
		assertEquals(0, Factory.wrap("Workshop").getMatcher().getFuzzyDistance(Factory.wrap("b")));
		assertEquals(1, Factory.wrap("Room").getMatcher().getFuzzyDistance(Factory.wrap("o")));
		assertEquals(1, Factory.wrap("Workshop").getMatcher().getFuzzyDistance(Factory.wrap("w")));
		assertEquals(2, Factory.wrap("Workshop").getMatcher().getFuzzyDistance(Factory.wrap("ws")));
		assertEquals(4, Factory.wrap("Workshop").getMatcher().getFuzzyDistance(Factory.wrap("wo")));
		assertEquals(3, Factory.wrap("Apache Software Foundation").getMatcher().getFuzzyDistance(Factory.wrap("asf")));
	}

	@Test
	public void testGetFuzzyScore_StringNullLocale() {
		try {
			Factory.EMPTY.getMatcher().getFuzzyDistance(null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
		}
	}

	/////////////// BITAP TESTS

	@Test
	public void bygFindExactMatchInMiddle() throws NoMatchException, IOException {
		SpanBuffer haystack = Factory.wrap("TGATGCATTCGTAGATGC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Matcher.Result result = haystack.getMatcher().match(needle);
		assertEquals(6, result.getIndex());
	}

	@Test
	public void bygFindExactMatchAtStart() throws NoMatchException, IOException {
		SpanBuffer haystack = Factory.wrap("ATTCGATGCATCAGTAGATGC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Matcher.Result result = haystack.getMatcher().match(needle);
		assertEquals(0, result.getIndex());

	}

	@Test
	public void bygFindExactMatchAtEnd() throws NoMatchException, IOException {
		SpanBuffer haystack = Factory.wrap("GATGCATCAGTAGATGCATTC");
		SpanBuffer needle = Factory.wrap("ATTC");
		Matcher.Result result = haystack.getMatcher().match(needle);
		assertEquals(17, result.getIndex());

	}

	@Test
	public void bygFindOverlappingExactMatches() throws NoMatchException, IOException {
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
		Matcher matcher = haystack.getMatcher();
		Matcher.Result result = matcher.match(needle);
		assertEquals(6, result.getIndex());
		result = matcher.match(needle, result);
		assertEquals(9, result.getIndex());
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
