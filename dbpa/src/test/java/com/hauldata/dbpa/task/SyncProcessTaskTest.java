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

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;

public class SyncProcessTaskTest extends TaskTest {

	public SyncProcessTaskTest(String name) {
		super(name);
	}

	public void testSyncProcessTask() throws Exception {

		String processId = "SyncProcessTest";
		String script = "TASK TEST PROCESS 'NESTED_PROCESS' 'fred' END TASK";

		String nestedScriptName = "NESTED_PROCESS";
		String nestedScript =
				"PARAMETERS name VARCHAR END PARAMETERS\n" +
				"TASK NESTED_TASK LOG name END TASK";

		Map<String, String> nestedScripts = new HashMap<String, String>();
		nestedScripts.put(nestedScriptName, nestedScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, nestedScripts, null); 

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		Analyzer.Record record = null;
		Analyzer.Record previousRecord = null;

		// Skip boilerplate

		record = recordIterator.next();
		record = recordIterator.next();

		// Analyze log of nested process

		String nestedProcessId = "SyncProcessTest[TEST].NESTED_PROCESS";

		previousRecord = record;
		record = recordIterator.next();
		assertEquals(nestedProcessId, record.processId);
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(0, record.level);
		assertEquals(DbProcess.startMessage, record.message);

		previousRecord = record;
		record = recordIterator.next();
		assertEquals(nestedProcessId, record.processId);
		assertEquals("NESTED_TASK", record.taskId);
		assertTrue(!record.datetime.isBefore(previousRecord.datetime));
		assertEquals(3, record.level);
		assertEquals("fred", record.message);

		previousRecord = record;
		record = recordIterator.next();
		assertEquals(nestedProcessId, record.processId);
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(!record.datetime.isBefore(previousRecord.datetime));
		assertEquals(0, record.level);
		assertEquals(DbProcess.completeMessage, record.message);

		previousRecord = record;
		record = recordIterator.next();
		assertEquals(nestedProcessId, record.processId);
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(!record.datetime.isBefore(previousRecord.datetime));
		assertEquals(3, record.level);
		assertTrue(record.message.startsWith(DbProcess.elapsedMessageStem));

		// Skip boilerplate

		record = recordIterator.next();
		record = recordIterator.next();
		record = recordIterator.next();

		assertFalse(recordIterator.hasNext());
	}
}
