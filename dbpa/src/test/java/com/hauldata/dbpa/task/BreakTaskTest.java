/*
 * Copyright (c) 2018, Ronald DeSantis
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

package com.hauldata.dbpa.task;

import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class BreakTaskTest extends TaskTest {

	public BreakTaskTest(String name) {
		super(name);
	}

	public void testBreakBadUsage() {

		String processId = "BreakBadUsageTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK Breaker BREAK END TASK \n" +
				"TASK LogAfter AFTER LOG 'After' END TASK \n" +
				"";

		boolean failed = false;
		try {
			Level logLevel = Level.info;
			boolean logToConsole = true;
			runScript(processId, logLevel, logToConsole, script, null, null, null);
		}
		catch (Exception ex) {
			failed = true;
			assertEquals("At line 2: BREAK may only appear in a looping construct", ex.getMessage());
		}
		assertTrue(failed);
	}

	public void testBreakDo() throws Exception {

		String processId = "BreakDoTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK Nester AFTER DO WHILE 1=1 \n" +
				"	TASK Breaker BREAK 'Should break' END TASK \n" +
				"	TASK NonBreaker LOG 'Should not break' END TASK \n" +
				"	TASK AfterNonBreaker AFTER NonBreaker LOG 'Also should not break' END TASK \n" +
				"END TASK \n" +
				"TASK LogAfter AFTER LOG 'After' END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, Pattern.compile("(NESTER)|(NESTER\\.BREAKER)|(LOGAFTER)"));
		record = recordIterator.next();
		assertEquals("NESTER", record.taskId);
		assertEquals(Task.startMessage, record.message);
		record = recordIterator.next();
		assertEquals("NESTER.BREAKER", record.taskId);
		assertEquals("Should break", record.message);
		record = recordIterator.next();
		assertEquals("NESTER.BREAKER", record.taskId);
		assertEquals(Task.breakingMessage, record.message);
		record = recordIterator.next();
		assertEquals("NESTER", record.taskId);
		assertEquals(Task.succeedMessage, record.message);
		record = recordIterator.next();
		assertEquals("LOGAFTER", record.taskId);
		assertEquals("After", record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, Pattern.compile("(NESTER)|(NESTER\\.NONBREAKER)|(NESTER\\.AFTERNONBREAKER)"));
		record = recordIterator.next();
		assertEquals("NESTER", record.taskId);
		assertEquals(Task.startMessage, record.message);
		record = recordIterator.next();
		assertEquals("NESTER.NONBREAKER", record.taskId);
		assertEquals("Should not break", record.message);
		record = recordIterator.next();
		assertEquals("NESTER.AFTERNONBREAKER", record.taskId);
		assertEquals("Also should not break", record.message);
		record = recordIterator.next();
		assertEquals("NESTER", record.taskId);
		assertEquals(Task.succeedMessage, record.message);
		assertFalse(recordIterator.hasNext());
	}

	public void testBreakForData() throws Exception {

		String processId = "BreakForDataTest";
		String script =
				"VARIABLES i INTEGER END VARIABLES \n" +
				"TASK Nester \n" +
				"	FOR i FROM VALUES (1), (2), (3), (4), (5)  \n" +
				"	TASK IF i = 3 BREAK END TASK \n" +
				"END TASK \n" +
				"TASK AFTER IF i <> 3 FAIL END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}

	public void testBreakSchedule() throws Exception {

		String processId = "BreakScheduleTest";
		String script =
				"VARIABLES i INTEGER END VARIABLES \n" +
				"TASK SET i = 0 END TASK \n" +
				"TASK Nester AFTER \n" +
				"	ON TODAY EVERY SECOND FROM NOW UNTIL 5 SECONDS FROM NOW  \n" +
				"	TASK IncrementIt SET i = i + 1 END TASK \n" +
				"	TASK BreakWhenAppropriate AFTER IF i = 3 BREAK END TASK \n" +
				"END TASK \n" +
				"TASK ConfirmBreak AFTER IF i <> 3 FAIL END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
