/*
 * Copyright (c) 2017, Ronald DeSantis
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

public class TaskIfTest extends TaskTest {

	public TaskIfTest(String name) {
		super(name);
	}

	public void testIf() throws Exception {

		String processId = "IfTest";
		String script =
				"VARIABLES result INTEGER END VARIABLES \n" +
				"TASK SET result = 6 END TASK \n" +
				"TASK Test1 AFTER IF (result / 2) % 2 = 1 LOG 'It worked' END TASK\n" +
				"TASK Test2 AFTER IF result % 2 = 0 LOG 'It worked' END TASK\n" +
				"TASK Test3 AFTER IF (result = 6) AND (5 <> result) LOG 'It worked' END TASK\n" +
				"TASK Test4 AFTER IF ('hell' + 'o w') + 'orld' = ('hello ' + ('world')) OR (result * 9) + 1 = 999 LOG 'It worked' END TASK\n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId);
		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals("TEST1", record.taskId);
		assertEquals("It worked", record.message);
		record = recordIterator.next();
		assertEquals("TEST2", record.taskId);
		assertEquals("It worked", record.message);
		record = recordIterator.next();
		assertEquals("TEST3", record.taskId);
		assertEquals("It worked", record.message);
		record = recordIterator.next();
		assertEquals("TEST4", record.taskId);
		assertEquals("It worked", record.message);

		record = recordIterator.next();
		assertEquals("process", record.taskId);
		assertFalse(recordIterator.hasNext());
	}
}
