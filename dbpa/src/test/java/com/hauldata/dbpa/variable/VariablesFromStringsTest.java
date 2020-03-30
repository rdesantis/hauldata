/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.variable;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

public class VariablesFromStringsTest extends TestCase {

	public VariablesFromStringsTest(String name) {
		super(name);
	}

	public void testBadInteger() {
		Variable<Integer> integer = new Variable<Integer>("integer", VariableType.INTEGER);
		String arg = "xxx";

		boolean isFailed = false;
		try {
			VariablesFromArguments.set(integer, arg);
		}
		catch (RuntimeException ex) {
			isFailed = true;
			assertEquals("Invalid integer format: xxx", ex.getMessage());
		}
		assertTrue(isFailed);
	}

	public void testBadDate() {
		Variable<LocalDateTime> datetime = new Variable<LocalDateTime>("datetime", VariableType.DATETIME);
		String arg = "yyy";

		boolean isFailed = false;
		try {
			VariablesFromArguments.set(datetime, arg);
		}
		catch (RuntimeException ex) {
			isFailed = true;
			assertEquals("Invalid date format: yyy", ex.getMessage());
		}
		assertTrue(isFailed);
	}

	public void testGood() {

		Variable<String> varchar = new Variable<String>("varchar", VariableType.VARCHAR);
		Variable<Integer> integer = new Variable<Integer>("integer", VariableType.INTEGER);
		Variable<Integer> bit = new Variable<Integer>("bit", VariableType.BIT);
		Variable<LocalDateTime> datetime = new Variable<LocalDateTime>("datetime", VariableType.DATETIME);

		List<VariableBase> variables = new LinkedList<VariableBase>();
		variables.add(varchar);
		variables.add(integer);
		variables.add(bit);
		variables.add(datetime);

		String[] args = new String[] {"hello", "123", "4567", "6/4/2017 12:55 AM"};

		VariablesFromArguments.set(variables, args);

		assertEquals("hello", varchar.getValue());
		assertEquals(123, integer.getValue().intValue());
		assertEquals(1, bit.getValue().intValue());
		assertEquals(LocalDateTime.of(2017, 6, 4, 0, 55, 0), datetime.getValue());
	}
}
