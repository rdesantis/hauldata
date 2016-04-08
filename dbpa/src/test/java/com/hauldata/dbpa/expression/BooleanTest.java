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

import com.hauldata.dbpa.expression.Expression.Combination;
import com.hauldata.dbpa.expression.Expression.Comparison;
import com.hauldata.dbpa.variable.VariableType;

import junit.framework.TestCase;

public class BooleanTest extends TestCase {

	static final Expression<Boolean> trueExpression = new Expression<Boolean>(VariableType.BOOLEAN) { public Boolean evaluate() { return true; }};
	static final Expression<Boolean> falseExpression = new Expression<Boolean>(VariableType.BOOLEAN) { public Boolean evaluate() { return false; }};

	public BooleanTest(String name) {
		super(name);
	}

	public void testBinary() {

		BooleanBinary binary;

		binary = new BooleanBinary(trueExpression, trueExpression, Combination.and);
		assertEquals(Boolean.TRUE, binary.evaluate());
		binary = new BooleanBinary(trueExpression, falseExpression, Combination.and);
		assertEquals(Boolean.FALSE, binary.evaluate());
		binary = new BooleanBinary(falseExpression, trueExpression, Combination.and);
		assertEquals(Boolean.FALSE, binary.evaluate());
		binary = new BooleanBinary(falseExpression, falseExpression, Combination.and);
		assertEquals(Boolean.FALSE, binary.evaluate());

		binary = new BooleanBinary(trueExpression, trueExpression, Combination.or);
		assertEquals(Boolean.TRUE, binary.evaluate());
		binary = new BooleanBinary(trueExpression, falseExpression, Combination.or);
		assertEquals(Boolean.TRUE, binary.evaluate());
		binary = new BooleanBinary(falseExpression, trueExpression, Combination.or);
		assertEquals(Boolean.TRUE, binary.evaluate());
		binary = new BooleanBinary(falseExpression, falseExpression, Combination.or);
		assertEquals(Boolean.FALSE, binary.evaluate());
	}

	public void testFromDatetimes() {

		final Expression<LocalDateTime> minDatetime = new DatetimeConstant(LocalDateTime.MIN);
		final Expression<LocalDateTime> maxDatetime = new DatetimeConstant(LocalDateTime.MAX);
		final Expression<LocalDateTime> nullDatetime = new DatetimeConstant(null);

		BooleanFromDatetimes comparison;

		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.lt);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.le);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, maxDatetime, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());

		comparison = new BooleanFromDatetimes(minDatetime, minDatetime, Comparison.eq);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.ge);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(maxDatetime, minDatetime, Comparison.gt);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromDatetimes(minDatetime, nullDatetime, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(minDatetime, nullDatetime, Comparison.ne);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(nullDatetime, maxDatetime, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(nullDatetime, maxDatetime, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(nullDatetime, nullDatetime, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(nullDatetime, nullDatetime, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromDatetimes(nullDatetime, nullDatetime, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
	}

	public void testFromIntegers() {

		final Expression<Integer> one = new IntegerConstant(1);
		final Expression<Integer> two = new IntegerConstant(2);
		final Expression<Integer> nullExpression = new IntegerConstant(null);

		BooleanFromIntegers comparison;

		comparison = new BooleanFromIntegers(one, two, Comparison.lt);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, two, Comparison.le);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, two, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, two, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, two, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, two, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());

		comparison = new BooleanFromIntegers(one, one, Comparison.eq);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromIntegers(two, one, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(two, one, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(two, one, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(two, one, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromIntegers(two, one, Comparison.ge);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromIntegers(two, one, Comparison.gt);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromIntegers(one, nullExpression, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(one, nullExpression, Comparison.ne);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(nullExpression, two, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(nullExpression, two, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(nullExpression, nullExpression, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(nullExpression, nullExpression, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromIntegers(nullExpression, nullExpression, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
	}

	public void testFromStrings() {

		final Expression<String> a = new StringConstant("a");
		final Expression<String> z = new StringConstant("z");
		final Expression<String> nullExpression = new StringConstant(null);

		BooleanFromStrings comparison;

		comparison = new BooleanFromStrings(a, z, Comparison.lt);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, z, Comparison.le);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, z, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, z, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, z, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, z, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());

		comparison = new BooleanFromStrings(a, a, Comparison.eq);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromStrings(z, a, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(z, a, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(z, a, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(z, a, Comparison.ne);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromStrings(z, a, Comparison.ge);
		assertEquals(Boolean.TRUE, comparison.evaluate());
		comparison = new BooleanFromStrings(z, a, Comparison.gt);
		assertEquals(Boolean.TRUE, comparison.evaluate());

		comparison = new BooleanFromStrings(a, nullExpression, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(a, nullExpression, Comparison.ne);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(nullExpression, z, Comparison.le);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(nullExpression, z, Comparison.ge);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(nullExpression, nullExpression, Comparison.lt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(nullExpression, nullExpression, Comparison.gt);
		assertEquals(Boolean.FALSE, comparison.evaluate());
		comparison = new BooleanFromStrings(nullExpression, nullExpression, Comparison.eq);
		assertEquals(Boolean.FALSE, comparison.evaluate());
	}

	public void testNot() {

		BooleanNot negation;

		negation = new BooleanNot(trueExpression);
		assertEquals(Boolean.FALSE, negation.evaluate());
		negation = new BooleanNot(falseExpression);
		assertEquals(Boolean.TRUE, negation.evaluate());
	}
}
