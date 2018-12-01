/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

import java.io.StringReader;
import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.process.TaskSet;

public class TaskSequenceTest extends TaskTest {

	public TaskSequenceTest(String name) {
		super(name);
	}

	public void testSequenceTasks() throws Exception {

		String processId = "SequenceTest";
		String script =
				"TASK LOG 'One' END TASK \n" +
				"TASK AFTER LOG 'Two' END TASK \n" +
				"TASK AFTER SUCCEEDS LOG 'Three' END TASK \n" +
				"TASK Four AFTER COMPLETES LOG 'Four' END TASK \n" +
				"TASK Five AFTER Four LOG 'Five' END TASK \n" +
				"TASK Six AFTER Five SUCCEEDS AND Four LOG 'Six' END TASK \n" +
				"TASK Seven AFTER Six COMPLETES LOG 'Seven' END TASK \n" +
				"TASK AFTER Seven COMPLETES LOG 'Eight' END TASK \n" +
				"TASK AFTER AND Six LOG 'Nine' END TASK \n" +
				"TASK AFTER AND Seven COMPLETES LOG 'Ten' END TASK \n" +
				"TASK AFTER PREVIOUS LOG 'Eleven' END TASK \n" +
				"TASK AFTER FAILS LOG 'Eleven failed' END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId);
		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.startMessage, record.message);

		record = recordIterator.next();
		assertEquals("1", record.taskId);
		assertEquals("One", record.message);
		record = recordIterator.next();
		assertEquals("2", record.taskId);
		assertEquals("Two", record.message);
		record = recordIterator.next();
		assertEquals("3", record.taskId);
		assertEquals("Three", record.message);
		record = recordIterator.next();
		assertEquals("FOUR", record.taskId);
		assertEquals("Four", record.message);
		record = recordIterator.next();
		assertEquals("FIVE", record.taskId);
		assertEquals("Five", record.message);
		record = recordIterator.next();
		assertEquals("SIX", record.taskId);
		assertEquals("Six", record.message);
		record = recordIterator.next();
		assertEquals("SEVEN", record.taskId);
		assertEquals("Seven", record.message);
		record = recordIterator.next();
		assertEquals("8", record.taskId);
		assertEquals("Eight", record.message);
		record = recordIterator.next();
		assertEquals("9", record.taskId);
		assertEquals("Nine", record.message);
		record = recordIterator.next();
		assertEquals("10", record.taskId);
		assertEquals("Ten", record.message);
		record = recordIterator.next();
		assertEquals("11", record.taskId);
		assertEquals("Eleven", record.message);
		record = recordIterator.next();
		assertEquals("12", record.taskId);
		assertEquals(TaskSet.orphanedMessage, record.message);

		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.completeMessage, record.message);
		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(record.message.startsWith(DbProcess.elapsedMessageStem));

		assertFalse(recordIterator.hasNext());
	}

	public void testSequenceOrTasks() throws Exception {

		String processId = "SequenceOrTest";
		String script =
				"TASK One GO END TASK \n" +
				"TASK Two AFTER GO END TASK \n" +
				"TASK Three AFTER GO END TASK \n" +
				"TASK CompleteSuccess AFTER GO END TASK \n" +
				"TASK TwoFails AFTER Two FAILS GO END TASK \n" +
				"TASK ThreeFails AFTER Three FAILS GO END TASK \n" +
				"TASK AllDone AFTER CompleteSuccess OR TwoFails OR ThreeFails GO END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, Pattern.compile("(ONE)|(TWO)|(THREE)|(COMPLETESUCCESS)|(ALLDONE)"));

		record = recordIterator.next();
		assertEquals("ONE", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("TWO", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("THREE", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("COMPLETESUCCESS", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("ALLDONE", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testFailSequenceOrTasks() throws Exception {

		String processId = "FailSequenceOrTest";
		String script =
				"TASK One GO END TASK \n" +
				"TASK Two AFTER GO END TASK \n" +
				"TASK Three AFTER FAIL END TASK \n" +
				"TASK CompleteSuccess AFTER GO END TASK \n" +
				"TASK TwoFails AFTER Two FAILS GO END TASK \n" +
				"TASK ThreeFails AFTER Three FAILS GO END TASK \n" +
				"TASK AllDone AFTER CompleteSuccess OR TwoFails OR ThreeFails GO END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, Pattern.compile("(ONE)|(TWO)|(THREE)|(THREEFAILS)|(ALLDONE)"));

		record = recordIterator.next();
		assertEquals("ONE", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("TWO", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("THREE", record.taskId);
		assertEquals(FailTask.failingMessage, record.message);
		record = recordIterator.next();
		assertEquals("THREE", record.taskId);
		assertEquals(Task.failMessage, record.message);
		record = recordIterator.next();
		assertEquals("THREEFAILS", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);
		record = recordIterator.next();
		assertEquals("ALLDONE", record.taskId);
		assertEquals(GoTask.continuingMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testAfterNothing() throws Exception {

		String script =
				"TASK AFTER GO END TASK \n" +
				"";

		boolean isFailed = false;
		try {
			DbProcess.parse(new StringReader(script));
		}
		catch (RuntimeException ex) {
			isFailed = true;
			assertEquals("At line 1: AFTER [PREVIOUS] with no previous task", ex.getMessage());
		}
		assertTrue(isFailed);
	}
}
