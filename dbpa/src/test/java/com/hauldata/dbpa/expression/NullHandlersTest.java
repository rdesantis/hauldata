/*
 * Copyright (c) 2016, Ronald DeSantis
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

package com.hauldata.dbpa.expression;

import java.time.LocalDateTime;

import junit.framework.TestCase;

public class NullHandlersTest extends TestCase {

	public NullHandlersTest(String name) {
		super(name);
	}

	static final Expression<LocalDateTime> minDatetime = new DatetimeConstant(LocalDateTime.MIN);
	static final Expression<LocalDateTime> maxDatetime = new DatetimeConstant(LocalDateTime.MAX);
	static final Expression<LocalDateTime> nullDatetime = new DatetimeConstant(null);

	static final Expression<Integer> one = new IntegerConstant(1);
	static final Expression<Integer> two = new IntegerConstant(2);
	static final Expression<Integer> nullInteger = new IntegerConstant(null);

	static final Expression<String> a = new StringConstant("a");
	static final Expression<String> z = new StringConstant("z");
	static final Expression<String> nullString = new StringConstant(null);

	public void testNullReplace() {

		NullReplace<LocalDateTime> datetimeReplacement;
		datetimeReplacement = new NullReplace<LocalDateTime>(minDatetime, maxDatetime);
		assertEquals(LocalDateTime.MIN, datetimeReplacement.evaluate());
		datetimeReplacement = new NullReplace<LocalDateTime>(minDatetime, nullDatetime);
		assertEquals(LocalDateTime.MIN, datetimeReplacement.evaluate());
		datetimeReplacement = new NullReplace<LocalDateTime>(nullDatetime, maxDatetime);
		assertEquals(LocalDateTime.MAX, datetimeReplacement.evaluate());
		datetimeReplacement = new NullReplace<LocalDateTime>(nullDatetime, nullDatetime);
		assertNull(datetimeReplacement.evaluate());

		NullReplace<Integer> integerReplacement;
		integerReplacement = new NullReplace<Integer>(one, two);
		assertEquals((Integer)1, integerReplacement.evaluate());
		integerReplacement = new NullReplace<Integer>(one, nullInteger);
		assertEquals((Integer)1, integerReplacement.evaluate());
		integerReplacement = new NullReplace<Integer>(nullInteger, two);
		assertEquals((Integer)2, integerReplacement.evaluate());
		integerReplacement = new NullReplace<Integer>(nullInteger, nullInteger);
		assertNull(integerReplacement.evaluate());

		NullReplace<String> stringReplacement;
		stringReplacement = new NullReplace<String>(a, z);
		assertEquals("a", stringReplacement.evaluate());
		stringReplacement = new NullReplace<String>(a, nullString);
		assertEquals("a", stringReplacement.evaluate());
		stringReplacement = new NullReplace<String>(nullString, z);
		assertEquals("z", stringReplacement.evaluate());
		stringReplacement = new NullReplace<String>(nullString, nullString);
		assertNull(stringReplacement.evaluate());
	}

	public void testNullTest() {
		
		NullTest nullTest;

		nullTest = new NullTest(minDatetime);
		assertFalse(nullTest.evaluate());
		nullTest = new NullTest(nullDatetime);
		assertTrue(nullTest.evaluate());

		nullTest = new NullTest(one);
		assertFalse(nullTest.evaluate());
		nullTest = new NullTest(nullInteger);
		assertTrue(nullTest.evaluate());

		nullTest = new NullTest(a);
		assertFalse(nullTest.evaluate());
		nullTest = new NullTest(nullString);
		assertTrue(nullTest.evaluate());
	}
}
