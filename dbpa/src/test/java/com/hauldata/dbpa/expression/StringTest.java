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
import java.time.format.DateTimeFormatter;

import junit.framework.TestCase;

public class StringTest extends TestCase {

	public StringTest(String name) {
		super(name);
	}

	public void testConcat() {

		final StringConstant left = new StringConstant("left");
		final StringConstant right = new StringConstant("right");
		final StringConstant nullString = new StringConstant(null);

		StringConcat concatenation;

		concatenation = new StringConcat(left, right);
		assertEquals("leftright", concatenation.evaluate());

		concatenation = new StringConcat(left, nullString);
		assertNull(concatenation.evaluate());
		concatenation = new StringConcat(nullString, right);
		assertNull(concatenation.evaluate());
		concatenation = new StringConcat(nullString, nullString);
		assertNull(concatenation.evaluate());
	}

	public void testFromDatetime() {

		final LocalDateTime christmas = LocalDateTime.of(2015, 12, 25, 1, 2, 3);
		final String pattern = "yyyy-MM-dd HH:mm:ss";

		StringFromDatetime fromDatetime;

		fromDatetime = new StringFromDatetime(new DatetimeConstant(christmas), new StringConstant(pattern));
		assertEquals(christmas.format(DateTimeFormatter.ofPattern(pattern)), fromDatetime.evaluate());

		fromDatetime = new StringFromDatetime(new DatetimeConstant(christmas), new StringConstant(null));
		assertNull(fromDatetime.evaluate());
		fromDatetime = new StringFromDatetime(new DatetimeConstant(null), new StringConstant(pattern));
		assertNull(fromDatetime.evaluate());
		fromDatetime = new StringFromDatetime(new DatetimeConstant(null), new StringConstant(null));
		assertNull(fromDatetime.evaluate());
	}

	public void testFromInteger() {

		StringFromInteger fromInteger;

		fromInteger = new StringFromInteger(new IntegerConstant(456789), new StringConstant("d"));
		assertEquals(456789, Integer.parseInt(fromInteger.evaluate()));

		fromInteger = new StringFromInteger(new IntegerConstant(456789), new StringConstant(null));
		assertNull(fromInteger.evaluate());
		fromInteger = new StringFromInteger(new IntegerConstant(null), new StringConstant("d"));
		assertNull(fromInteger.evaluate());
		fromInteger = new StringFromInteger(new IntegerConstant(null), new StringConstant(null));
		assertNull(fromInteger.evaluate());
	}
}
