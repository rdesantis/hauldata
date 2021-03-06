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
import com.hauldata.dbpa.process.TaskSet;

public class DoTaskTest extends TaskTest {

	public DoTaskTest(String name) {
		super(name);
	}

	public void testDoLoopTask() throws Exception {

		String processId = "DoLoopTest";
		String script =
				"VARIABLES datewhen DATETIME END VARIABLES \n" +
				"TASK SetWhen SET datewhen = '12/1/2015' END TASK \n" + // 2015-12-01 is a Tuesday, DATEPART(WEEKDAY,...) = 3
				"TASK Loop AFTER SetWhen DO WHILE datewhen < '12/8/2015' \n" +
					"TASK Echo LOG FORMAT(datewhen, 'yyyy-MM-dd') + ' ' + " +
						"FORMAT(DATEPART(YEAR, datewhen), 'd') + ' ' + " +
						"FORMAT(DATEPART(MONTH, datewhen), 'd') + ' ' + " +
						"FORMAT(DATEPART(DAY, datewhen), 'd') + ' ' + " +
						"FORMAT(DATEPART(WEEKDAY, datewhen), 'd') END TASK \n" +
					"TASK Increment AFTER Echo SET datewhen = DATEADD(DAY, 1, datewhen) END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");
		Analyzer.Record record = null;
		String[] parts = null;

		record = recordIterator.next();
		parts = record.message.split(" ");
		assertEquals("2015-12-01", parts[0]);
		assertEquals("2015", parts[1]);
		assertEquals("12", parts[2]);
		assertEquals("1", parts[3]);
		assertEquals("3", parts[4]);

		record = recordIterator.next();
		parts = record.message.split(" ");
		assertEquals("2015-12-02", parts[0]);
		assertEquals("2015", parts[1]);
		assertEquals("12", parts[2]);
		assertEquals("2", parts[3]);
		assertEquals("4", parts[4]);

		record = recordIterator.next();
		record = recordIterator.next();
		record = recordIterator.next();
		record = recordIterator.next();

		record = recordIterator.next();
		parts = record.message.split(" ");
		assertEquals("2015-12-07", parts[0]);
		assertEquals("2015", parts[1]);
		assertEquals("12", parts[2]);
		assertEquals("7", parts[3]);
		assertEquals("2", parts[4]);

		assertFalse(recordIterator.hasNext());
	}

	public void testDoOnceTask() throws Exception {

		String processId = "DoOnceTest";
		String script =
				"TASK Once DO\n" +
					"TASK Echo1 LOG 'One' END TASK \n" +
					"TASK Echo2 LOG 'Two' END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, "ONCE.ECHO1");
		record = recordIterator.next();
		assertEquals("ONCE.ECHO1", record.taskId);
		assertEquals("One", record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "ONCE.ECHO2");
		record = recordIterator.next();
		assertEquals("ONCE.ECHO2", record.taskId);
		assertEquals("Two", record.message);
		assertFalse(recordIterator.hasNext());
	}

	public void testDeepDoFail() throws Exception {

		String processId = "DeepDoFail";
		String script =
				"VARIABLES counter INTEGER END VARIABLES\n" +
				"TASK One FOR counter FROM VALUES (1), (2), (3)\n" +

//				"	TASK Two IF counter = 2 \n" +
//				"		FAIL 'Fail ' + FORMAT(counter, 'd') \n" +
//				"	END TASK \n" +

				"	TASK Two IF counter = 2 DO\n" +
				"		TASK LOG 'Two.1' END TASK \n" +
				"		TASK Failure AFTER FAIL 'Deep Fail ' + FORMAT(counter, 'd') END TASK \n" +
				"	END TASK \n" +

				"	TASK Happy AFTER Two SUCCEEDS LOG 'Happy ' + FORMAT(counter, 'd') END TASK \n" +	// THIS FAILS
				"	TASK Catcher AFTER Two FAILS LOG 'Caught ' + FORMAT(counter, 'd') END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator;
		Analyzer.Record record;

		recordIterator = analyzer.recordIterator(processId, "ONE.HAPPY");
		record = recordIterator.next();
		assertEquals("Happy 1", record.message);
		record = recordIterator.next();
		assertEquals(TaskSet.orphanedMessage, record.message);
		record = recordIterator.next();
		assertEquals("Happy 3", record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "ONE.CATCHER");
		record = recordIterator.next();
		assertEquals(TaskSet.orphanedMessage, record.message);
		record = recordIterator.next();
		assertEquals("Caught 2", record.message);
		record = recordIterator.next();
		assertEquals(TaskSet.orphanedMessage, record.message);
		assertFalse(recordIterator.hasNext());
	}
}
