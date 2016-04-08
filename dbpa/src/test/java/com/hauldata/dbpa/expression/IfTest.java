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

import com.hauldata.dbpa.variable.VariableType;

import junit.framework.TestCase;

public class IfTest extends TestCase {

	static final Expression<Boolean> trueExpression = new Expression<Boolean>(VariableType.BOOLEAN) { public Boolean evaluate() { return true; }};
	static final Expression<Boolean> falseExpression = new Expression<Boolean>(VariableType.BOOLEAN) { public Boolean evaluate() { return false; }};

	public IfTest(String name) {
		super(name);
	}

	public void testDatetime() {

		final DatetimeConstant minDatetime = new DatetimeConstant(LocalDateTime.MIN);
		final DatetimeConstant maxDatetime = new DatetimeConstant(LocalDateTime.MAX);

		IfExpression<LocalDateTime> ifExpression;

		ifExpression = new IfExpression<LocalDateTime>(trueExpression, minDatetime, maxDatetime);
		assertEquals(LocalDateTime.MIN, ifExpression.evaluate());
		assertFalse(LocalDateTime.MAX.equals(ifExpression.evaluate()));

		ifExpression = new IfExpression<LocalDateTime>(falseExpression, minDatetime, maxDatetime);
		assertEquals(LocalDateTime.MAX, ifExpression.evaluate());
		assertFalse(LocalDateTime.MIN.equals(ifExpression.evaluate()));
	}

	public void testInteger() {

		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant two = new IntegerConstant(2);

		IfExpression<Integer> ifExpression;

		ifExpression = new IfExpression<Integer>(trueExpression, one, two);
		assertEquals(1, (int)ifExpression.evaluate());
		assertFalse(2 == ifExpression.evaluate());

		ifExpression = new IfExpression<Integer>(falseExpression, one, two);
		assertEquals(2, (int)ifExpression.evaluate());
		assertFalse(1 == ifExpression.evaluate());
	}

	public void testString() {

		final StringConstant one = new StringConstant("one");
		final StringConstant two = new StringConstant("two");

		IfExpression<String> ifExpression;

		ifExpression = new IfExpression<String>(trueExpression, one, two);
		assertEquals("one", ifExpression.evaluate());
		assertFalse("two".equals(ifExpression.evaluate()));

		ifExpression = new IfExpression<String>(falseExpression, one, two);
		assertEquals("two", ifExpression.evaluate());
		assertFalse("one".equals(ifExpression.evaluate()));
	}
}
