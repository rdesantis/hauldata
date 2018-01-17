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

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class UpdateVariablesTest extends TaskTest {

	public UpdateVariablesTest(String name) {
		super(name);
	}

	public void testUpdateVariables() throws Exception {

		String processId = "UpdateVariablesTest";
		String script =
				"VARIABLES i INT, v VARCHAR, b BIT END VARIABLES \n" +
				"TASK UPDATE i, v, b FROM VALUES (1, 'one', 1) END TASK \n" +
				"TASK Echo AFTER LOG FORMAT(i, 'd') + ',' + v + ',' + FORMAT(b, 'd') END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;
		String[] parts;

		recordIterator = analyzer.recordIterator(processId, "ECHO");

		record = recordIterator.next();
		parts = record.message.split(",");
		assertEquals(3, parts.length);
		assertEquals("1", parts[0]);
		assertEquals("one", parts[1]);
		assertEquals("1", parts[2]);

		assertFalse(recordIterator.hasNext());
	}

	public void testUpdateVariablesNegative() throws Exception {

		String processId = "UpdateVariablesNegativeTest";
		String script =
				"VARIABLES i INT, v VARCHAR, b BIT END VARIABLES \n" +
				"TASK UpdateThem UPDATE i, v, b FROM VALUES (1, 'one', 1, 'garbage') END TASK \n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;
		Analyzer analyzer = null;

		analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null, false);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator(processId, "UPDATETHEM");

		record = recordIterator.next();
		assertEquals("Database query returned different number of columns than variables to update", record.message);

		record = recordIterator.next();
		assertEquals(Task.failMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testForValues() throws Exception {

		String processId = "ForTableTest";
		String script =
				"VARIABLES i INT, v VARCHAR, b BIT END VARIABLES \n" +
				"TASK Loop FOR i, v, b FROM VALUES (1, 'one', 1), (2, 'two', 0) \n" +
					"TASK Echo LOG FORMAT(i, 'd') + ',' + v + ',' + FORMAT(b, 'd') END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;
		String[] parts;

		recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");

		record = recordIterator.next();
		parts = record.message.split(",");
		assertEquals(3, parts.length);
		assertEquals("1", parts[0]);
		assertEquals("one", parts[1]);
		assertEquals("1", parts[2]);

		record = recordIterator.next();
		parts = record.message.split(",");
		assertEquals(3, parts.length);
		assertEquals("2", parts[0]);
		assertEquals("two", parts[1]);
		assertEquals("0", parts[2]);

		assertFalse(recordIterator.hasNext());
	}

	public void testForValuesNegative() throws Exception {

		String processId = "ForTableTestNegative";
		String script =
				"VARIABLES i INT, v VARCHAR, b BIT END VARIABLES \n" +
				"TASK Loop FOR i, v, b FROM SQL SELECT 1, 'one', 1, '' \n" +
					"TASK Echo LOG FORMAT(i, 'd') + ',' + v + ',' + FORMAT(b, 'd') END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;
		Analyzer analyzer = null;

		analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null, false);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator(processId, "LOOP");

		record = recordIterator.next();
		assertEquals("Database query returned different number of columns than variables to update", record.message);

		record = recordIterator.next();
		assertEquals(Task.failMessage, record.message);

		assertFalse(recordIterator.hasNext());
	}
}
