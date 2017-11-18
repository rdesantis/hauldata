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

package com.hauldata.dbpa.process;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.task.TaskTest;

public class TaskSetTest extends TaskTest {

	public TaskSetTest(String name) {
		super(name);
	}

	public void testNulls() throws Exception {

		String processId = "NullsTest";
		String script =
				"PARAMETERS increment INTEGER END PARAMETERS \n" +
				"VARIABLES \n" +
					"future DATETIME, something VARCHAR, three INTEGER, \n" +
					"nullint INT, notnullint INT, \n" +
					"nullchar VARCHAR, notnullchar VARCHAR, \n" +
					"nulldate DATE, notnulldate DATE \n" +
				"END VARIABLES \n" +
				"TASK Assign SET \n" +
					"future = DATEADD(MONTH, increment, GETDATE()), something = 'Some Thing', three = 3, \n" +
					"notnullint = 1, \n" +
					"notnullchar = 'stuff', \n" +
					"notnulldate = GETDATE() \n" +
				"END TASK \n" +
				"TASK log1 AFTER Assign LOG 'Time ' + FORMAT(increment, 'd') + ' months from now is ' + FORMAT(future, 'M/d/yyyy h:mm:ss a') END TASK \n" +
				"TASK log2 AFTER log1 COMPLETES LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log3 AFTER log2 LOG 'notnullint IS ' + IIF(notnullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log4 AFTER log3 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log5 AFTER log4 LOG 'notnullchar IS ' + IIF(notnullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log6 AFTER log5 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log7 AFTER log6 LOG 'notnulldate IS ' + IIF(notnulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK AssignNull AFTER log7 SET \n" +
					"nullint = NULL, \n" +
					"nullchar = null, \n" +
					"nulldate = Null \n" +
				"END TASK \n" +
				"TASK log12 AFTER AssignNull LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log14 AFTER log12 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log16 AFTER log14 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK ComputeNull AFTER log16 SET \n" +
					"nullint = NULL + 1, \n" +
					"nullchar = 'fred' + null, \n" +
					"nulldate = DATEFROMPARTS(2016, 1, nullint) \n" +
				"END TASK \n" +
				"TASK log22 AFTER ComputeNull LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log24 AFTER log22 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log26 AFTER log24 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log32 AFTER log26 LOG 'nullint expression IS ' + IIF(DATEPART(YEAR, nulldate) IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log34 AFTER log32 LOG 'nullchar expression IS ' + IIF(FORMAT(nullint, 'd') IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log36 AFTER log34 LOG 'nulldate expression IS ' + IIF(DATEADD(DAY, nullint, GETDATE()) IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK Nester AFTER log36 PROCESS 'whatever' WITH nullchar + 'x', 1 + 2 END TASK \n" +
				"";

		String nestedScriptName = "whatever";
		String nestedScript =
				"PARAMETERS parm1 VARCHAR, parm2 INT END PARAMETERS\n" +
				"TASK nest1 LOG 'nullchar IS ' + IIF(parm1 IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK nest2 AFTER nest1 LOG 'notnullint IS ' + IIF(parm2 IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"";

		Map<String, String> nestedScripts = new HashMap<String, String>();
		nestedScripts.put(nestedScriptName, nestedScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, nestedScripts, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, Pattern.compile("LOG\\d+$"));
		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals("LOG1", record.taskId);
		assertEquals("Message evaluates to NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG1", record.taskId);
		assertEquals(Task.failMessage, record.message);

		record = recordIterator.next();
		assertEquals("LOG2", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG3", record.taskId);
		assertEquals("notnullint IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG4", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG5", record.taskId);
		assertEquals("notnullchar IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG6", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG7", record.taskId);
		assertEquals("notnulldate IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG12", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG14", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG16", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG22", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG24", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG26", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG32", record.taskId);
		assertEquals("nullint expression IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG34", record.taskId);
		assertEquals("nullchar expression IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG36", record.taskId);
		assertEquals("nulldate expression IS NULL", record.message);

		assertFalse(recordIterator.hasNext());

		String nestedProcessId = processId + "." + nestedScriptName;

		recordIterator = analyzer.recordIterator(nestedProcessId, Pattern.compile("NEST\\d+$"));

		record = recordIterator.next();
		assertEquals("NEST1", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("NEST2", record.taskId);
		assertEquals("notnullint IS NOT NULL", record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testDateFormat() throws Exception {

		String processId = "DateFormatTest";
		String script =
				"VARIABLES \n" +
					"d1 DATETIME, d2 DATETIME, d3 DATETIME, \n" +
					"s1 VARCHAR, s2 VARCHAR, s3 VARCHAR, \n" +
					"i1 INTEGER, i2 INTEGER, i3 INTEGER \n" +
				"END VARIABLES \n" +
				"TASK DateFormat SET \n" +
					"d1 = '9/1/2015 9:30 PM', \n" +
					"d2 = '2001-8-10 12:34:56', \n" +
					"d3 = '2015-09-05T00:40:26.090' \n" +
				"END TASK \n" +
				"TASK Echo AFTER DateFormat LOG \n" +
					"'d1 = ' + FORMAT(d1,'yyyy-MM-dd HH:mm:ss') + ', d2 = ' + FORMAT(d2,'yyyy-MM-dd') + ', d3 = ' + FORMAT(d3,'M/d/yyyy h:mm:ss a') \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();
		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals("ECHO", record.taskId);
		assertEquals("d1 = 2015-09-01 21:30:00, d2 = 2001-08-10, d3 = 9/5/2015 12:40:26 AM", record.message);
	}

	public void testSchedule() throws Exception {

		String processId = "ScheduleTest";
		String script =
				"VARIABLES occurs VARCHAR, start DATETIME, finish DATETIME, ms_format VARCHAR, no_ms_format VARCHAR END VARIABLES \n" +
				"TASK Scheduler ON TODAY NOW \n" +
					"TASK Schedulee LOG 'Scheduled task' END TASK \n" +
				"END TASK \n" +
				"TASK SetSchedule SET occurs = 'TODAY NOW' END TASK \n" +
				"TASK Scheduler2 AFTER Scheduler AND SetSchedule ON SCHEDULE occurs \n" +
					"TASK Schedulee2 LOG 'Another scheduled task' END TASK \n" +
				"END TASK \n" +
				"TASK SetRange AFTER Scheduler2 SET \n" +
					"start = DATEADD(SECOND, 2, GETDATE()), \n" +
					"finish = DATEADD(SECOND, 6, start), \n" +
					"occurs = 'TODAY NOW, TODAY EVERY 2 SECONDS FROM ''' + FORMAT(start, 'HH:mm:ss') + ''' UNTIL ''' + FORMAT(finish, 'HH:mm:ss') + '''', \n" +
					"no_ms_format = 'uuuu-MM-dd''T''HH:mm:ss', \n" +
					"ms_format = no_ms_format + '.SSS' \n" +
				"END TASK \n" +
				"TASK ShowSchedule AFTER SetRange LOG occurs END TASK \n" +
				"TASK ShowTimes AFTER ShowSchedule LOG FORMAT(GETDATE(), ms_format) + ',' + FORMAT(start, no_ms_format) + ',' + FORMAT(finish, no_ms_format) END TASK \n" +
				"TASK Scheduler3 AFTER ShowTimes ON SCHEDULE occurs \n" +
					"TASK Schedulee3 LOG 'Recurring scheduled task' END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		int frequency = 2;
		int cycles = 3;

		final int fuzzMillis = 250;
		final float fuzzSeconds = (float)fuzzMillis / 1000f;

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator("ScheduleTest", "SHOWTIMES");
		Analyzer.Record record = recordIterator.next();

		LocalDateTime messageTime = record.datetime;
		String[] times = record.message.split(",");
		LocalDateTime getdateTime = LocalDateTime.parse(times[0]);
		LocalDateTime startTime = LocalDateTime.parse(times[1]);
		LocalDateTime finishTime = LocalDateTime.parse(times[2]);

		assertEquals(0f, secondsBetween(messageTime, getdateTime), fuzzSeconds);
		assertEquals((float)frequency, secondsBetween(getdateTime, startTime), 1f);

		recordIterator = analyzer.recordIterator("ScheduleTest", "SCHEDULER3.SCHEDULEE3");
		String recurMessage = "Recurring scheduled task";

		record = recordIterator.next();
		assertEquals(recurMessage, record.message);
		assertEquals(0f, secondsBetween(getdateTime, record.datetime), fuzzSeconds);

		record = recordIterator.next();
		assertEquals(recurMessage, record.message);
		assertEquals(0f, secondsBetween(startTime, record.datetime), fuzzSeconds);

		LocalDateTime previousTime = record.datetime;
		for (int cycle = 0; cycle < cycles; ++cycle) {

			record = recordIterator.next();
			assertEquals(recurMessage, record.message);
			assertEquals((float)frequency, secondsBetween(previousTime, record.datetime), fuzzSeconds);

			previousTime = record.datetime;
		}

		assertEquals(0f, secondsBetween(finishTime, record.datetime), fuzzSeconds);

		assertFalse(recordIterator.hasNext());
	}

	private static float secondsBetween(LocalDateTime earlier, LocalDateTime later) {
		return (float)ChronoUnit.MILLIS.between(earlier, later) / 1000f;
	}

	public void testTaskSyntax() {
		String script;

		script = "TASK WRITE LOG 'fail' END TASK \n";
		assertBadSyntax(script, null);

		script = "TASK NAME READ LOG 'succeed' END TASK \n";
		assertGoodSyntax(script);

		assertReservedTaskName("PREVIOUS");
		assertReservedTaskName("SUCCEEDS");
		assertReservedTaskName("FAILS");
		assertReservedTaskName("COMPLETES");
		assertReservedTaskName("IF");
		assertReservedTaskName("NAME");
		assertReservedTaskName("AFTER");
		assertReservedTaskName("END");
		assertReservedTaskName("TASK");
	}

	private void assertReservedTaskName(String name) {
		String script = "TASK NAME " + name + " LOG 'fail' END TASK \n";
		String message = "At line 1: Cannot use reserved name as a TASK name: " + name;
		assertBadSyntax(script, message);
	}

	public void testVariableNaming() {

		assertReservedVariableName("NULL");
		assertReservedVariableName("CASE");
		assertReservedVariableName("END");
		assertReservedVariableName("TASK");

		assertGoodVariableName("WHEN");
		assertGoodVariableName("YEAR");
		assertGoodVariableName("MONTH");
		assertGoodVariableName("DAY");
		assertGoodVariableName("WEEKDAY");
		assertGoodVariableName("HOUR");
		assertGoodVariableName("MINUTE");
		assertGoodVariableName("SECOND");

		String script;
		String message;

		script =
				"VARIABLES one INTEGER, one VARCHAR END VARIABLES\n" +
				"TASK LOG 'fail' END TASK\n";
		message = "At line 1: Duplicate variable name: ONE";
		assertBadSyntax(script, message);

		script =
				"VARIABLES one THING END VARIABLES\n" +
				"TASK LOG 'fail' END TASK\n";
		message = "At line 1: Invalid variable type name: THING";
		assertBadSyntax(script, message);
	}

	private void assertReservedVariableName(String name) {
		String script =
				"VARIABLES " + name + " INTEGER END VARIABLES\n" +
				"TASK LOG 'fail' END TASK\n";
		String message = "At line 1: Cannot use reserved word as a variable name: " + name;
		assertBadSyntax(script, message);
	}

	private void assertGoodVariableName(String name) {
		String script =
				"VARIABLES " + name + " INTEGER END VARIABLES\n" +
				"TASK LOG 'succeed' END TASK\n";
		assertGoodSyntax(script);
	}

	public void testConnectionNaming() {

		assertReservedConnectionName("CONNECTION");
		assertReservedConnectionName("DEFAULT");
		assertReservedConnectionName("STATEMENT");
		assertReservedConnectionName("SQL");
		assertReservedConnectionName("TABLE");
		assertReservedConnectionName("SCRIPT");
		assertReservedConnectionName("VALUES");
		assertReservedConnectionName("PROCEDURE");
		assertReservedConnectionName("FILE");
		assertReservedConnectionName("FILES");
		assertReservedConnectionName("VARIABLE");
		assertReservedConnectionName("END");
		assertReservedConnectionName("TASK");

		assertGoodConnectionName("DATABASE");
		assertGoodConnectionName("whatever");

		String script;
		String message;

		script =
				"CONNECTIONS the FTP, the EMAIL END CONNECTIONS\n" +
				"TASK LOG 'fail' END TASK\n";
		message = "At line 1: Duplicate connection name: THE";
		assertBadSyntax(script, message);

		script =
				"VARIABLES a DATETIME END VARIABLES\n" +
				"CONNECTIONS a DATABASE END CONNECTIONS\n" +
				"TASK LOG 'fail' END TASK\n";
		message = "At line 2: Connection name cannot be the same as a variable name: A";
		assertBadSyntax(script, message);

		script =
				"CONNECTIONS the HOUSE END CONNECTIONS\n" +
				"TASK LOG 'fail' END TASK\n";
		message = "At line 1: Invalid connection type name: HOUSE";
		assertBadSyntax(script, message);
	}

	private void assertReservedConnectionName(String name) {
		String script =
				"CONNECTIONS " + name + " DATABASE END CONNECTIONS\n" +
				"TASK LOG 'fail' END TASK\n";
		String message = "At line 1: Cannot use reserved word as a connection name: " + name;
		assertBadSyntax(script, message);
	}

	private void assertGoodConnectionName(String name) {
		String script =
				"CONNECTIONS " + name + " DATABASE END CONNECTIONS\n" +
				"TASK LOG 'succeed' END TASK\n";
		assertGoodSyntax(script);
	}

	public void testFunctionSyntax() {
		String script;
		String message;

		script =
				"VARIABLES isnull INTEGER END VARIABLES\n" +
				"TASK SET isnull = 1 END TASK\n";
		assertGoodSyntax(script);

		script =
				"VARIABLES x INTEGER END VARIABLES\n" +
				"TASK SET x = ISNULL(x, 0) END TASK\n";
		assertGoodSyntax(script);

		script =
				"VARIABLES x INTEGER END VARIABLES\n" +
				"TASK SET x = ISNULL (x, 0) END TASK\n";
		message = "At line 2: Invalid INTEGER expression term: ISNULL";
		assertBadSyntax(script, message);
	}
}
