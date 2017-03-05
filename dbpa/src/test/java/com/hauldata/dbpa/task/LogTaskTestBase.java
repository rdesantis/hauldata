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

import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;

public abstract class LogTaskTestBase extends TaskTest {

	protected LogTaskTestBase(String name) {
		super(name);
	}

	protected void testLogTask(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script,
			String logMessage) throws Exception {

		List<String> logMessages = new LinkedList<String>();
		logMessages.add(logMessage);

		testLogTask(processId, logLevel, logToConsole, script, logMessages);
	}

	protected void testLogTask(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script,
			List<String> logMessages) throws Exception {

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		Analyzer.Record record = null;

		if (logLevel.compareTo(Level.info) <= 0) {
			record = recordIterator.next();
			assertEquals(processId, record.processId);
			assertEquals(DbProcess.processTaskId, record.taskId);
			assertEquals(0, record.level);
			assertEquals(DbProcess.startMessage, record.message);
		}

		Analyzer.Record previousRecord = null;

		for (String logMessage : logMessages) {

			if (logLevel.compareTo(Level.message) <= 0) {
				previousRecord = record;
				record = recordIterator.next();
				assertEquals(processId, record.processId);
				assertEquals("TEST", record.taskId);
				assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
				assertEquals(3, record.level);
				assertEquals(logMessage, record.message);
			}
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
