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

public class ConstantTest extends TestCase {

	public ConstantTest(String name) {
		super(name);
	}

	public void testDatetime() {

		final DatetimeConstant minDatetime = new DatetimeConstant(LocalDateTime.MIN);
		final DatetimeConstant alsoMinDatetime = new DatetimeConstant(LocalDateTime.MIN);

		assertEquals(LocalDateTime.MIN, minDatetime.evaluate());
		assertEquals(minDatetime, alsoMinDatetime);
	}

	public void testInteger() {

		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant alsoOne = new IntegerConstant(1);

		assertEquals((Integer)1, one.evaluate());
		assertEquals(one, alsoOne);
	}

	public void testString() {

		final StringConstant one = new StringConstant("one");
		final StringConstant alsoOne = new StringConstant("one");

		assertEquals("one", one.evaluate());
		assertEquals(one, alsoOne);
	}
}
