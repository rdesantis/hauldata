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

package com.hauldata.dbpa.task;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;

public class AsyncProcessTaskTest extends TaskTest {

	private static final String processId = "AsyncProcessTest";

	private static Analyzer analyzer = null;

	public AsyncProcessTaskTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {

		if (analyzer != null) return;

		final String script =
				"TASK TEST1 PROCESS ASYNC 'NESTED_PROCESS1' 'ethel' END TASK\n" +
				"TASK TEST2 PROCESS ASYNC 'NESTED_PROCESS2' 123 END TASK\n" +
				"TASK NAME WAIT AFTER TEST1 AND TEST2 WAITFOR ASYNC END TASK";

		final String nestedScript1 =
				"PARAMETERS name VARCHAR END PARAMETERS\n" +
				"TASK NESTED_TASK1 LOG name END TASK\n" +
				"TASK NAME PAUSE1 AFTER WAITFOR DELAY '0:00:02' END TASK";

		final String nestedScript2 =
				"PARAMETERS number INT END PARAMETERS\n" +
				"TASK NESTED_TASK2 LOG FORMAT(number, 'd') END TASK\n" +
				"TASK NAME PAUSE2 AFTER WAITFOR DELAY '0:00:04' END TASK";

		Map<String, String> nestedScripts = new HashMap<String, String>();
		nestedScripts.put("NESTED_PROCESS1", nestedScript1);
		nestedScripts.put("NESTED_PROCESS2", nestedScript2);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		analyzer = runScript(processId, logLevel, logToConsole, script, null, nestedScripts, null);
	}

	private void testAsyncProcessLaunch(String taskId) throws Exception {

		String taskId_async = taskId + "-async";

		Analyzer.RecordIterator iterator = analyzer.recordIterator(processId, Pattern.compile(taskId + "(\\-async)?"));
		Analyzer.Record record;

		record = iterator.next();
		assertEquals(taskId, record.taskId);
		assertEquals(Task.startMessage, record.message);

		boolean gotSyncComplete = false;
		boolean gotAsyncStart = false;
		while (!gotSyncComplete || !gotAsyncStart) {

			record = iterator.next();
			boolean haveSyncComplete = (record.taskId.equals(taskId)) && (record.message.equals(Task.succeedMessage));
			boolean haveAsyncStart = (record.taskId.equals(taskId_async)) && (record.message.equals(Task.startMessage));

			assertTrue(haveSyncComplete || haveAsyncStart);
			assertFalse(haveSyncComplete && gotSyncComplete);
			assertFalse(haveAsyncStart && gotAsyncStart);

			gotSyncComplete = gotSyncComplete || haveSyncComplete;
			gotAsyncStart = gotAsyncStart || haveAsyncStart;
		}

		record = iterator.next();
		assertEquals(taskId_async, record.taskId);
		assertEquals(Task.succeedMessage, record.message);

		assertFalse(iterator.hasNext());
	}

	private void testAsyncProcessRun(String parentTaskId, String nestedProcessId, String taskId, String message, String pauseTaskId) throws Exception {

		String qualifiedProcessId = processId + "[" + parentTaskId + "-async]." + nestedProcessId;

		Analyzer.RecordIterator iterator = analyzer.recordIterator(qualifiedProcessId);
		Analyzer.Record record;

		record = iterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.startMessage, record.message);

		record = iterator.next();
		assertEquals(taskId, record.taskId);
		assertEquals(message, record.message);

		record = iterator.next();
		assertEquals(pauseTaskId, record.taskId);
		assertEquals(Task.startMessage, record.message);

		record = iterator.next();
		assertEquals(pauseTaskId, record.taskId);
		assertEquals(Task.succeedMessage, record.message);

		record = iterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.completeMessage, record.message);

		record = iterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(record.message.startsWith(DbProcess.elapsedMessageStem));

		assertFalse(iterator.hasNext());
	}

	public void testAsyncProcessLaunch() throws Exception {
		testAsyncProcessLaunch("TEST1");
		testAsyncProcessLaunch("TEST2");
	}

	public void testAsyncProcessRun() throws Exception {
		testAsyncProcessRun("TEST1", "NESTED_PROCESS1", "NESTED_TASK1", "ethel", "PAUSE1");
		testAsyncProcessRun("TEST2", "NESTED_PROCESS2", "NESTED_TASK2", "123", "PAUSE2");
	}

	public void testAsyncProcessSyntax() {
		String script;

		final String missingPredecessorMessage = "At line %d: WAITFOR ASYNC TASK must have direct or indirect predecessor or predecessor descendent that is PROCESS ASYNC TASK";
		final String invalidCombinationMessage = "At line %d: WAITFOR ASYNC TASK or direct or indirect predecessor cannot combine predecessors with OR";

		script =
				"TASK Parent1 DO\n" +
				"	TASK GO END TASK\n" +
				"	TASK AFTER PROCESS ASYNC 'dummy' END TASK\n" +
				"	TASK AFTER GO END TASK\n" +
				"END TASK\n" +
				"TASK Parent2 AFTER Parent1 DO\n" +
				"	TASK WAITFOR ASYNC END TASK\n" +
				"END TASK\n";

		assertGoodSyntax(script);

		script =
				"TASK UnrelatedAsync PROCESS ASYNC 'dummy' END TASK\n" +
				"TASK Parent1 DO\n" +
				"	TASK GO END TASK\n" +
				"	TASK AFTER PROCESS SYNC 'dummy' END TASK\n" +
				"	TASK AFTER GO END TASK\n" +
				"END TASK\n" +
				"TASK Parent2 AFTER Parent1 DO\n" +
				"	TASK WAITFOR ASYNC END TASK\n" +
				"END TASK\n";

		assertBadSyntax(script, String.format(missingPredecessorMessage, 8));

		script =
				"TASK AnotherAsync PROCESS ASYNC 'dummy' END TASK\n" +
				"TASK Parent1 DO\n" +
				"	TASK GO END TASK\n" +
				"	TASK AFTER PROCESS ASYNC 'dummy' END TASK\n" +
				"	TASK AFTER GO END TASK\n" +
				"END TASK\n" +
				"TASK Parent2 AFTER Parent1 OR AnotherAsync DO\n" +
				"	TASK WAITFOR ASYNC END TASK\n" +
				"END TASK\n";

		assertBadSyntax(script, String.format(invalidCombinationMessage, 8));

		script =
				"TASK AnotherAsync PROCESS ASYNC 'dummy' END TASK\n" +
				"TASK Parent1 DO\n" +
				"	TASK GO END TASK\n" +
				"	TASK AFTER PROCESS ASYNC 'dummy' END TASK\n" +
				"	TASK AFTER GO END TASK\n" +
				"END TASK\n" +
				"TASK Parent2 AFTER Parent1 AND AnotherAsync DO\n" +
				"	TASK WAITFOR ASYNC END TASK\n" +
				"END TASK\n";

		assertGoodSyntax(script);
	}
}
