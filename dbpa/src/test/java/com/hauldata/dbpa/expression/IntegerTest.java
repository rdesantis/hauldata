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
import java.time.temporal.ChronoField;

import com.hauldata.dbpa.expression.IntegerBinary.Operator;

import junit.framework.TestCase;

public class IntegerTest extends TestCase {

	public void testBinary() {

		final IntegerConstant one = new IntegerConstant(1);
		final IntegerConstant two = new IntegerConstant(2);
		final IntegerConstant three = new IntegerConstant(3);
		final IntegerConstant four = new IntegerConstant(4);
		final IntegerConstant six = new IntegerConstant(6);
		final IntegerConstant nullExpression = new IntegerConstant(null);

		IntegerBinary binary;

		binary = new IntegerBinary(one, two, Operator.add);
		assertEquals((Integer)3, binary.evaluate());
		binary = new IntegerBinary(three, two, Operator.subtract);
		assertEquals((Integer)1, binary.evaluate());
		binary = new IntegerBinary(two, three, Operator.multiply);
		assertEquals((Integer)6, binary.evaluate());
		binary = new IntegerBinary(six, three, Operator.divide);
		assertEquals((Integer)2, binary.evaluate());
		binary = new IntegerBinary(four, three, Operator.modulo);
		assertEquals((Integer)1, binary.evaluate());

		binary = new IntegerBinary(one, nullExpression, Operator.add);
		assertNull(binary.evaluate());
		binary = new IntegerBinary(nullExpression, two, Operator.subtract);
		assertNull(binary.evaluate());
		binary = new IntegerBinary(nullExpression, nullExpression, Operator.multiply);
		assertNull(binary.evaluate());
	}

	public void testFromDatetime() {
		
		final Expression<LocalDateTime> datetime = new DatetimeConstant(LocalDateTime.of(2016, 4, 6, 10, 32, 15));
		final Expression<LocalDateTime> nullExpression = new DatetimeConstant(null);
		
		IntegerFromDatetime datepart;
		
		datepart = new IntegerFromDatetime(datetime, ChronoField.YEAR);
		assertEquals((Integer)2016, datepart.evaluate());
		datepart = new IntegerFromDatetime(datetime, ChronoField.MONTH_OF_YEAR);
		assertEquals((Integer)4, datepart.evaluate());
		datepart = new IntegerFromDatetime(datetime, ChronoField.DAY_OF_MONTH);
		assertEquals((Integer)6, datepart.evaluate());
		datepart = new IntegerFromDatetime(datetime, ChronoField.HOUR_OF_DAY);
		assertEquals((Integer)10, datepart.evaluate());
		datepart = new IntegerFromDatetime(datetime, ChronoField.MINUTE_OF_HOUR);
		assertEquals((Integer)32, datepart.evaluate());
		datepart = new IntegerFromDatetime(datetime, ChronoField.SECOND_OF_MINUTE);
		assertEquals((Integer)15, datepart.evaluate());

		datepart = new IntegerFromDatetime(nullExpression, ChronoField.YEAR);
		assertNull(datepart.evaluate());
	}
}
