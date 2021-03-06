/*
 * Copyright (c) 2020, Ronald DeSantis
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

public class TableVariableTest extends TaskTest {

	public TableVariableTest(String name) {
		super(name);
	}

	public void testInsert() throws Exception {

		String processId = "InsertTest";
		String script =
				"PROCESS \n" +
				"VARIABLES i INT, v VARCHAR, b BIT, rows TABLE; \n" +
				"SET i = 1, v = 'one', b = 0; \n" +
				"INSERT INTO rows VALUES (i, v, b), (2, 'two', 1), (i+2, 'the' + 'ee', IIF(b = 1, 0, 1)); \n" +
				"FOR i, v, b FROM VARIABLE rows \n" +
				"	LOG FORMAT(i, 'd') + ' ' + v + ' ' + IIF(b = 1, '1', '0'); \n" +
				"END FOR \n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator();

		record = recordIterator.next();
		assertEquals("1 one 0", record.message);
		record = recordIterator.next();
		assertEquals("2 two 1", record.message);
		record = recordIterator.next();
		assertEquals("3 theee 1", record.message);
		record = recordIterator.next();

		assertFalse(recordIterator.hasNext());
	}

	public void testTruncate() throws Exception {

		String processId = "TruncateTest";
		String script =
				"PROCESS \n" +
				"VARIABLES ii INT, vv VARCHAR, bb BIT, v TABLE; \n" +
				"INSERT INTO v VALUES (3, 'two', 1); \n" +
				"TRUNCATE v; \n" +
				"FOR ii, vv, bb FROM VARIABLE v \n" +
				"	LOG FORMAT(ii, 'd') + ' ' + vv + ' ' + IIF(bb = 1, '1', '0'); \n" +
				"END FOR \n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;

		recordIterator = analyzer.recordIterator();

		recordIterator.next();

		assertFalse(recordIterator.hasNext());
	}

	public void testSetTableVariable() throws Exception {

		String processId = "SetTableVariableTest";
		String script =
				"PROCESS \n" +
				"VARIABLES ii INT, vv VARCHAR, bb BIT, v TABLE, w TABLE; \n" +
				"INSERT INTO v VALUES (3, 'two', 1); \n" +
				"SET w = v; \n" +
				"FOR ii, vv, bb FROM VARIABLE w \n" +
				"	LOG FORMAT(ii, 'd') + ' ' + vv + ' ' + IIF(bb = 1, '1', '0'); \n" +
				"END FOR \n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator();

		record = recordIterator.next();
		assertEquals("3 two 1", record.message);
		record = recordIterator.next();

		assertFalse(recordIterator.hasNext());
	}

	public void testPassTableVariable() throws Exception {

		String processId = "PassTableVariableTest";
		String script =
				"PROCESS \n" +
				"VARIABLES v TABLE; \n" +
				"INSERT INTO v VALUES (3, 'two', 1); \n" +
				"RUN PROCESS 'TakeTable' v; \n" +
				"END PROCESS\n" +
				"PROCESS TakeTable\n" +
				"PARAMETERS w TABLE; \n" +
				"VARIABLES ii INT, vv VARCHAR, bb BIT; \n" +
				"FOR ii, vv, bb FROM VARIABLE w \n" +
				"	LOG FORMAT(ii, 'd') + ' ' + vv + ' ' + IIF(bb = 1, '1', '0'); \n" +
				"END FOR \n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.message;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator();

		record = recordIterator.next();
		assertEquals("3 two 1", record.message);
		record = recordIterator.next();
		record = recordIterator.next();

		assertFalse(recordIterator.hasNext());
	}

	public void testValuesFunction() throws Exception {

		String processId = "ValuesFunctionTest";
		String script =
				"PROCESS \n" +
				"DECLARE i INTEGER, v VARCHAR, t TABLE = VALUES((1, 'one'), (2, 'two')); \n" +
				"FOR i, v FROM VARIABLE t \n" +
				"	OF FORMAT(i, 'd') + ' ' + v:\n" +
				"	DECLARE expected VARCHAR =\n" +
				"		CASE i\n" +
				"		WHEN 1 THEN 'one'\n" +
				"		WHEN 2 THEN 'two'\n" +
				"		ELSE NULL\n" +
				"		END;\n" +
				"	IF expected IS NULL OR v <> expected FAIL;\n" +
				"END FOR \n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}

	public void testBadSyntax() throws Exception {
		String script;

		script =
				"PROCESS \n" +
				"VARIABLES v TABLE; \n" +
				"SET v = 1; \n" +
				"END PROCESS\n" +
				"";

		assertBadSyntax(script, "At line 3: Invalid TABLE expression term: 1");

		script =
				"PROCESS \n" +
				"VARIABLES v TABLE, w TABLE; \n" +
				"INSERT INTO w VARIABLE v; \n" +
				"INSERT INTO v VARIABLE v; \n" +
				"END PROCESS\n" +
				"";

		assertBadSyntax(script, "At line 4: Cannot INSERT a variable into itself");
	}
}
