package com.hauldata.dbpa.task;

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

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null); 

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

		if (logLevel.compareTo(Level.message) <= 0) {
			previousRecord = record;
			record = recordIterator.next();
			assertEquals(processId, record.processId);
			assertEquals("TEST", record.taskId);
			assertTrue(previousRecord == null || !record.datetime.isBefore(previousRecord.datetime));
			assertEquals(3, record.level);
			assertEquals(logMessage, record.message);
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
