/*
 * Copyright (c) 2020, Ronald DeSantis
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.task.TaskTest;
import com.hauldata.dbpa.variable.Table;
import com.hauldata.dbpa.variable.VariableType;

public class CallTest extends TaskTest {

	private static final String className = "com.hauldata.dbpa.expression.call.CallableSample";

	public CallTest(String name) {
		super(name);
	}

	public void testRaw() {
		final StringConstant classNameConstant = new StringConstant(className);
		final List<ExpressionBase> noArguments = new LinkedList<ExpressionBase>();

		List<ExpressionBase> arguments = new LinkedList<ExpressionBase>();
		Integer integerResult;
		Boolean booleanResult;
		String stringResult;
		LocalDateTime dateTimeResult;
		Table tableResult;

		integerResult = new Call<Integer>(
				VariableType.INTEGER,
				classNameConstant,
				new StringConstant("bitTrue"),
				noArguments).evaluate();

		assertEquals((Integer)1, integerResult);

		booleanResult = new Call<Boolean>(
				VariableType.BOOLEAN,
				classNameConstant,
				new StringConstant("booleanTrue"),
				noArguments).evaluate();

		assertEquals((Boolean)true, booleanResult);

		integerResult = new Call<Integer>(
				VariableType.INTEGER,
				classNameConstant,
				new StringConstant("five"),
				noArguments).evaluate();

		assertEquals((Integer)5, integerResult);

		arguments.clear();
		arguments.add(new IntegerConstant(11));
		arguments.add(new IntegerConstant(22));

		integerResult = new Call<Integer>(
				VariableType.INTEGER,
				classNameConstant,
				new StringConstant("add"),
				arguments).evaluate();

		assertEquals((Integer)33, integerResult);

		stringResult = new Call<String>(
				VariableType.VARCHAR,
				classNameConstant,
				new StringConstant("fred"),
				noArguments).evaluate();

		assertEquals("fred", stringResult);

		arguments.clear();
		arguments.add(new StringConstant("wombatty"));
		arguments.add(new IntegerConstant(4));

		stringResult = new Call<String>(
				VariableType.VARCHAR,
				classNameConstant,
				new StringConstant("left"),
				arguments).evaluate();

		assertEquals("womb", stringResult);

		LocalDateTime before = LocalDateTime.now(); 
		dateTimeResult = new Call<LocalDateTime>(
				VariableType.DATETIME,
				classNameConstant,
				new StringConstant("now"),
				noArguments).evaluate();
		LocalDateTime after = LocalDateTime.now(); 

		assertTrue(!dateTimeResult.isBefore(before));
		assertTrue(!dateTimeResult.isAfter(after));

		arguments.clear();
		arguments.add(new DatetimeConstant(after));
		arguments.add(new IntegerConstant(7));

		dateTimeResult = new Call<LocalDateTime>(
				VariableType.DATETIME,
				classNameConstant,
				new StringConstant("plusDays"),
				arguments).evaluate();

		assertEquals(after.plusDays(7), dateTimeResult);

		tableResult = new Call<Table>(
				VariableType.TABLE,
				classNameConstant,
				new StringConstant("stuff"),
				noArguments).evaluate();

		ArrayList<List<Object>> rows = (ArrayList<List<Object>>)tableResult.getValuesLists();
		assertEquals(2, rows.size());
		ArrayList<Object> row0 = (ArrayList<Object>) rows.get(0);
		ArrayList<Object> row1 = (ArrayList<Object>) rows.get(1);

		assertEquals("hello", row0.get(0));
		assertEquals((Integer)123, row0.get(1));
		assertEquals("world", row1.get(0));
		assertEquals((Integer)456, row1.get(1));

		arguments.clear();
		arguments.add(new Constant<Table>(VariableType.TABLE, tableResult));

		integerResult = new Call<Integer>(
				VariableType.INTEGER,
				classNameConstant,
				new StringConstant("sum"),
				arguments).evaluate();

		assertEquals((Integer)579, integerResult);
	}

	public void testScripted() throws Exception {
		String processId = "CallScriptedTest";
		String script =
				"PROCESS \n" +
				"VARIABLES i INTEGER, v VARCHAR, d DATETIME, t TABLE; \n" +
				"IF CALL('" + className + "', 'bitTrue') <> 1 FAIL 'bitTrue';\n" +
				"IF CALL('" + className + "', 'five') <> 5 FAIL 'five';\n" +
				"IF CALL('" + className + "', 'add', 321, 654) <> 975 FAIL 'add';\n" +
				"SET v = CALL('" + className + "', 'fred');\n" +
				"IF  v <> 'fred' FAIL 'fred';\n" +
				"SET v = CALL('" + className + "', 'left', 'something', 4);\n" +
				"IF  v <> 'some' FAIL 'left';\n" +
				"setd: SET d = DATEADD(DAY, 0, CALL('" + className + "', 'now'));\n" +
				"testd: IF d > GETDATE() FAIL 'now';\n" +
//				"IF CALL('" + className + "', 'plusDays', '1/1/2021', 1) <> DATEADD(DAY, 1, '1/1/2021') FAIL 'plusDays';\n" +
				"END PROCESS \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
