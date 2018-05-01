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

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.process.TaskSet;

public class StopTaskTest extends TaskTest {

	public StopTaskTest(String name) {
		super(name);
	}

	public void testStopLinear() throws Exception {

		String processId = "DoLinearTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK Stopper AFTER STOP 'Middle' END TASK \n" +
				"TASK LogAfter AFTER LOG 'After' END TASK \n" +
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
		assertEquals("LOGBEFORE", record.taskId);
		assertEquals("Before", record.message);

		record = recordIterator.next();
		assertEquals("STOPPER", record.taskId);
		assertEquals("Middle", record.message);

		record = recordIterator.next();
		assertEquals("STOPPER", record.taskId);
		assertEquals(Task.stopMessage, record.message);

		record = recordIterator.next();
		assertEquals("LOGAFTER", record.taskId);
		assertEquals(TaskSet.orphanedMessage, record.message);

		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.stopMessage, record.message);

		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);

		assertFalse(recordIterator.hasNext());
	}

	public void testStopConditional() throws Exception {

		String processId = "DoConditionalTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK NonStopper AFTER IF 1=0 STOP 'Should not stop' END TASK \n" +
				"TASK Stopper AFTER IF 1=1 STOP 'Should stop' END TASK \n" +
				"TASK LogAfter AFTER LOG 'After' END TASK \n" +
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
		assertEquals("LOGBEFORE", record.taskId);
		assertEquals("Before", record.message);

		record = recordIterator.next();
		assertEquals("NONSTOPPER", record.taskId);
		assertEquals(Task.skipMessage, record.message);

		record = recordIterator.next();
		assertEquals("STOPPER", record.taskId);
		assertEquals("Should stop", record.message);

		record = recordIterator.next();
		assertEquals("STOPPER", record.taskId);
		assertEquals(Task.stopMessage, record.message);

		record = recordIterator.next();
		assertEquals("LOGAFTER", record.taskId);
		assertEquals(TaskSet.orphanedMessage, record.message);

		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.stopMessage, record.message);

		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);

		assertFalse(recordIterator.hasNext());
	}

	public void testStopConcurrent() throws Exception {

		String processId = "DoConcurrentTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK NonStopper AFTER LogBefore LOG 'Should not stop' END TASK \n" +
				"TASK Stopper AFTER LogBefore STOP 'Should stop' END TASK \n" +
				"TASK LogAfter AFTER NonStopper COMPLETES AND Stopper COMPLETES LOG 'After' END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, "LOGAFTER");
		record = recordIterator.next();
		assertEquals(TaskSet.orphanedMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testStopNested() throws Exception {

		String processId = "DoNestedTest";
		String script =
				"TASK LogBefore LOG 'Before' END TASK \n" +
				"TASK Nester AFTER DO \n" +
				"	TASK Stopper STOP 'Should stop' END TASK \n" +
				"	TASK NonStopper LOG 'Should not stop' END TASK \n" +
				"	TASK AfterNonStopper AFTER NonStopper LOG 'Also should not stop' END TASK \n" +
				"END TASK \n" +
				"TASK LogAfter AFTER LOG 'After' END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, "NESTER");
		record = recordIterator.next();
		assertEquals(Task.startMessage, record.message);

		record = recordIterator.next();
		assertEquals(Task.stopMessage, record.message);

		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "LOGAFTER");
		record = recordIterator.next();
		assertEquals(TaskSet.orphanedMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}
}
