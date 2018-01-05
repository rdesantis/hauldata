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

public class ForDataTaskTest extends TaskTest {

	public ForDataTaskTest(String name) {
		super(name);
	}

	/* MySQL stored proc:
	DELIMITER //
	CREATE PROCEDURE test.passer (INOUT bumped INT, i INT, v VARCHAR(255), d DATETIME, b BIT)
	BEGIN
		SELECT i, v, d, b;
	    SET bumped = bumped + 1;
	END//
	*/

	public void testForProcedure() throws Exception {

		String processId = "ForProcedureTest";
		String script =
				"VARIABLES anInt INT, aString VARCHAR, aDateTime DATETIME, aBit BIT, five INT, now DATETIME, one BIT END VARIABLES \n" +
				"TASK SET five = 5, now = GETDATE(), one = 1 END TASK \n" +
				"TASK Loop AFTER FOR anInt, aString, aDateTime, aBit FROM PROCEDURE 'test.passer' WITH 0, five + 4 IN, 'What have you!', now, one \n" +
					"TASK Echo LOG FORMAT(anInt, 'd') + ',''' + aString + ''',' + FORMAT(aDateTime, 'yyyy-MM-dd HH:mm:ss') + ',' + FORMAT(aBit, 'd') END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");
		Analyzer.Record record = null;

		record = recordIterator.next();
		String[] parts = record.message.split(",");
		assertEquals(4, parts.length);
		assertEquals("9", parts[0]);
		assertEquals("'What have you!'", parts[1]);
		assertEquals("1", parts[3]);

		assertFalse(recordIterator.hasNext());
	}

	public void testForProcedureInOut() throws Exception {

		String processId = "ForProcedureInOutTest";
		String script =
				"VARIABLES anInt INT, aString VARCHAR, aDateTime DATETIME, aBit BIT, bumpee INT, now DATETIME END VARIABLES \n" +
				"TASK SET bumpee = 5, now = GETDATE() END TASK \n" +
				"TASK Loop AFTER FOR anInt, aString, aDateTime, aBit FROM PROCEDURE 'test.passer' WITH bumpee INOUT, 3 * 7, 'non' + ('sense'), now, 0 \n" +
				"TASK Echo LOG FORMAT(anInt, 'd') + ',''' + aString + ''',' + FORMAT(aDateTime, 'yyyy-MM-dd HH:mm:ss') + ',' + FORMAT(aBit, 'd') END TASK \n" +
				"END TASK \n" +
				"TASK Changed AFTER LOG FORMAT(bumpee, 'd') END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");

		record = recordIterator.next();
		String[] parts = record.message.split(",");
		assertEquals(4, parts.length);
		assertEquals("21", parts[0]);
		assertEquals("'nonsense'", parts[1]);
		assertEquals("0", parts[3]);

		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "CHANGED");

		record = recordIterator.next();
		assertEquals("6", record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testForProcedureSyntax() throws Exception {

		String rawScript =
				"VARIABLES anInt INT, aString VARCHAR END VARIABLES \n" +
				"TASK Loop FOR anInt FROM PROCEDURE aString WITH anInt + 1 %s \n" +
					"TASK GO END TASK \n" +
				"END TASK \n" +
				"";

		String script;
		String message;

		script = String.format(rawScript,  "OUT");
		message = "At line 2: OUT or INOUT argument must be a variable";

		assertBadSyntax(script, message);

		script = String.format(rawScript,  "INOUT");
		message = "At line 2: OUT or INOUT argument must be a variable";

		assertBadSyntax(script, message);

		script = String.format(rawScript,  "IN");

		assertGoodSyntax(script);
	}

	public void testForTable() throws Exception {

		String processId = "ForTableTest";
		String script =
				"VARIABLES id INT, name VARCHAR, description VARCHAR, size INT END VARIABLES \n" +
						"TASK Loop FOR name, description, size FROM TABLE 'test.things' \n" +
					"TASK Echo LOG name + ', ' + description + ', ' + FORMAT(size, 'd') END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
