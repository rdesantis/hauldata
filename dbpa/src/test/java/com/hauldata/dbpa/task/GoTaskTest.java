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

import java.util.ListIterator;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;

public class GoTaskTest extends LogTaskTestBase {

	public GoTaskTest(String name) {
		super(name);
	}

	public void testGoMessageTask() throws Exception {

		String processId = "GoMessageTest";
		String script = "TASK TEST GO 'We have a go' END TASK";

		testLogTask(processId, Level.info, true, script, "We have a go");
		testLogTask(processId, Level.message, true, script, "We have a go");
	}

	public void testGoNakedTask() throws Exception {

		String processId = "GoNakedTest";
		String script = "TASK TEST GO END TASK";

		testGoNakedTask(processId, Level.info, true, script);
		testGoNakedTask(processId, Level.message, true, script);
	}

	private void testGoNakedTask(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script) throws Exception {

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null); 

		ListIterator<Analyzer.Record> recordIterator = analyzer.getRecords().listIterator();
		
		Analyzer.Record record = null;

		if (logLevel.compareTo(Level.info) <= 0) {
			record = recordIterator.next();
			assertEquals(processId, record.processId);
			assertEquals(DbProcess.processTaskId, record.taskId);
			assertEquals(0, record.level);
			assertEquals(DbProcess.startMessage, record.message);
		}

		Analyzer.Record previousRecord = null;

		if (logLevel.compareTo(Level.info) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals(processId, record.processId);
			assertEquals("TEST", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(0, record.level);
			assertEquals(GoTask.continueMessage, record.message);
		}

		if (logLevel.compareTo(Level.info) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals(processId, record.processId);
			assertEquals(DbProcess.processTaskId, record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(0, record.level);
			assertEquals(DbProcess.completeMessage, record.message);
		}

		previousRecord = record;
		record = recordIterator.next();
		assertEquals(processId, record.processId);
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
		assertEquals(3, record.level);
		assertTrue(record.message.startsWith(DbProcess.elapsedMessageStem));

		assertFalse(recordIterator.hasNext());
	}
}
