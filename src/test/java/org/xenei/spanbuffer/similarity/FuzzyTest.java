package org.xenei.spanbuffer.similarity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

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
