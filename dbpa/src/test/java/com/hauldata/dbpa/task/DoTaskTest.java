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

public class DoTaskTest extends TaskTest {

	public DoTaskTest(String name) {
		super(name);
	}

	public void testDoLoopTask() throws Exception {

		String processId = "DoLoopTest";
		String script = 
				"VARIABLES when DATETIME END VARIABLES \n" +
				"TASK SetWhen SET when = '12/1/2015' END TASK \n" + // 2015-12-01 is a Tuesday, DATEPART(WEEKDAY,...) = 3
				"TASK Loop AFTER SetWhen DO WHILE when < '12/8/2015' \n" +
					"TASK Echo LOG FORMAT(when, 'yyyy-MM-dd') + ' ' + " +
						"FORMAT(DATEPART(YEAR, when), 'd') + ' ' + " +
						"FORMAT(DATEPART(MONTH, when), 'd') + ' ' + " +
						"FORMAT(DATEPART(DAY, when), 'd') + ' ' + " +
						"FORMAT(DATEPART(WEEKDAY, when), 'd') END TASK \n" +
					"TASK Increment AFTER Echo SET when = DATEADD(DAY, 1, when) END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null); 

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

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null); 

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
}
