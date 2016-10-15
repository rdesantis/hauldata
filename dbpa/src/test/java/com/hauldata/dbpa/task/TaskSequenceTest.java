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
				"TASK AFTER FAILS LOG 'Ten failed' END TASK \n" +
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
		assertEquals(TaskSet.orphanedMessage, record.message);

	
		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertEquals(DbProcess.completeMessage, record.message);
		record = recordIterator.next();
		assertEquals(DbProcess.processTaskId, record.taskId);
		assertTrue(record.message.startsWith(DbProcess.elapsedMessageStem));
		
		assertFalse(recordIterator.hasNext());
	}
}
