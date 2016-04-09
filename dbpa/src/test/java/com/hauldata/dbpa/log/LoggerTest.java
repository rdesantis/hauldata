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

package com.hauldata.dbpa.log;

import java.util.ListIterator;

import com.hauldata.dbpa.log.Logger.Level;

import junit.framework.TestCase;

public class LoggerTest extends TestCase {

	public LoggerTest(String name) {
		super(name);
	}

	private Analyzer generateLog(Level level) {
		
		RootLogger logger = new RootLogger("testProcess", level);

		Analyzer analyzer = new Analyzer();
		logger.add(analyzer);

		logger.info("firstTask", "firstMessage");
		logger.warn("secondTask", "secondMessage");
		logger.error("thirdTask", "thirdMessage");
		logger.message("fourthTask", "fourthMessage");

		Logger nestedTaskLogger = logger.nestTask("nestedTask");

		nestedTaskLogger.message("firstNestedTask", "fifthMessage");

		Logger nestedProcessLogger = logger.nestProcess("nestedProcess");

		nestedProcessLogger.message("secondNestedTask", "sixthMessage");

		logger.close();
		
		return analyzer;
	}

	private void testLogger(Level level) {

		// Generate a log.

		Analyzer analyzer = generateLog(level);

		// Analyze the log.

		ListIterator<Analyzer.Record> recordIterator = analyzer.getRecords().listIterator();
		
		Analyzer.Record record = null;

		if (level.compareTo(Level.info) <= 0) {
			record = recordIterator.next();
			assertEquals("testProcess", record.processId);
			assertEquals("firstTask", record.taskId);
			assertEquals(0, record.level);
			assertEquals("firstMessage", record.message);
		}

		Analyzer.Record previousRecord = null;

		if (level.compareTo(Level.warn) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals("testProcess", record.processId);
			assertEquals("secondTask", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(1, record.level);
			assertEquals("secondMessage", record.message);
		}

		if (level.compareTo(Level.error) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals("testProcess", record.processId);
			assertEquals("thirdTask", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(2, record.level);
			assertEquals("thirdMessage", record.message);
		}

		if (level.compareTo(Level.message) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals("testProcess", record.processId);
			assertEquals("fourthTask", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(3, record.level);
			assertEquals("fourthMessage", record.message);
		}

		if (level.compareTo(Level.message) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals("testProcess", record.processId);
			assertEquals("nestedTask.firstNestedTask", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(3, record.level);
			assertEquals("fifthMessage", record.message);
		}

		if (level.compareTo(Level.message) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals("testProcess.nestedProcess", record.processId);
			assertEquals("secondNestedTask", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(3, record.level);
			assertEquals("sixthMessage", record.message);
		}
		
		assertFalse(recordIterator.hasNext());
	}

	public void testLogger() {
		testLogger(Level.info);
		testLogger(Level.warn);
		testLogger(Level.error);
		testLogger(Level.message);
	}
}
