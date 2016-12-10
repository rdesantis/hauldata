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

import com.hauldata.dbpa.DbProcessTestTables;
import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.Context;

public class ReadTaskTest extends TaskTest {

	public ReadTaskTest(String name) {
		super(name);
	}

	private static ContextAction assureTestTablesExist = new ContextAction() { public void action(Context context) { DbProcessTestTables.assureExist(context); } };

	public void testRead() throws Exception {

		String processId = "ReadTest";
		String script =
				"TASK ReadCsv0 RUN SQL TRUNCATE TABLE test.importtarget END TASK \n" +
				"TASK ReadCsv1 AFTER ReadCsv0 COMPLETES READ CSV 'import no header.csv' WITH NO HEADERS INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsv2 AFTER ReadCsv1 COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsv3 AFTER ReadCsv2 COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsv4 AFTER ReadCsv3 COMPLETES READ CSV 'badtrips.csv' WITH HEADERS INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsv5 AFTER FailCsv4 COMPLETES READ CSV 'read file.csv' WITH HEADERS 'Wrong', 'Headers' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsv6 AFTER FailCsv5 COMPLETES READ CSV 'read file.csv' WITH HEADERS 'Not Enough Headers' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsv7 AFTER FailCsv6 COMPLETES READ CSV 'read file.csv' WITH HEADERS 'Too', 'Many', 'Headers' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsv8 AFTER FailCsv7 COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' INTO TABLE 'test.importtarget' END TASK \n" +
				"TASK ReadCsv9 AFTER ReadCsv8 COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS INTO TABLE 'test.importtarget'  END TASK \n" +
				"TASK ReadCsvA AFTER ReadCsv9 COMPLETES READ CSV 'import arbitrary header.csv' IGNORE HEADERS INTO TABLE 'test.importtarget' END TASK \n" +
				"TASK ReadCsvB AFTER ReadCsvA COMPLETES READ CSV 'import metadata header.csv' IGNORE HEADERS INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +

				"TASK ReadCsvC AFTER ReadCsvB COMPLETES READ CSV 'import no header.csv' WITH NO HEADERS COLUMNS 1, 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsvD AFTER ReadCsvC COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 1, 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsvE AFTER ReadCsvD COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS (2+1), 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsvF AFTER FailCsvE COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 'Arbitrary','Random Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsvG AFTER ReadCsvF COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 'Arbitrary','Wrong'+' Runtime Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsvH AFTER FailCsvG COMPLETES READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random ' + 'Stuff' COLUMNS 'Arbitrary','Wrong Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsvI AFTER FailCsvH COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS COLUMNS 1, 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsvJ AFTER ReadCsvI COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS COLUMNS 1, 3 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsvK AFTER FailCsvJ COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS COLUMNS 'number', 'word' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK FailCsvL AFTER ReadCsvK COMPLETES READ CSV 'import metadata header.csv' WITH HEADERS COLUMNS 'more numb', 'word' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +

				"TASK ReadCsvM AFTER FailCsvL COMPLETES READ CSV 'EH0010_20151109_20151115.csv' COLUMNS 'request_outcome', 6 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK ReadCsvN AFTER ReadCsvM COMPLETES READ CSV 'EH0010_20151102_20151108.csv' IGNORE HEADERS COLUMNS 2, 'app' INTO TABLE 'test.importtarget' END TASK \n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, assureTestTablesExist);
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		assertNextTaskFailed(recordIterator, "FAILCSV4", "The file has wrong number of columns for the requested operation");
		assertNextTaskFailed(recordIterator, "FAILCSV5", "File column headers do not match those specified");
		assertNextTaskFailed(recordIterator, "FAILCSV6", "File column headers do not match those specified");
		assertNextTaskFailed(recordIterator, "FAILCSV7", "File column headers do not match those specified");
		assertNextTaskFailed(recordIterator, "FAILCSVE", "Attempting to read column beyond bounds of record at position");
		assertNextTaskFailed(recordIterator, "FAILCSVG", "Attempting to read column with non-existing header");
		assertNextTaskFailed(recordIterator, "FAILCSVH", "Attempting to read column with non-existing header");
		assertNextTaskFailed(recordIterator, "FAILCSVJ", "Attempting to read column beyond bounds of record at position");
		assertNextTaskFailed(recordIterator, "FAILCSVL", "Attempting to read column with non-existing header");

		recordIterator.next();
		assertFalse(recordIterator.hasNext());
	}

	private void assertNextTaskFailed(Analyzer.RecordIterator recordIterator, String taskId, String messageStem) {

		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals(record.taskId, taskId);
		assertTrue(record.message.startsWith(messageStem));
		record = recordIterator.next();
		assertEquals(record.taskId, taskId);
		assertEquals(record.message, Task.failMessage);
	}

	public void testReadPrefix() throws Exception {

		String processId = "ReadPrefixTest";
		String script =
				"VARIABLES number INT, word VARCHAR END VARIABLES\n" +
				"TASK PopulateTable RUN SQL\n" +
				"	TRUNCATE TABLE test.importtarget;\n" +
				"	INSERT INTO test.importtarget VALUES (321, 'splunge');\n" +
				"END TASK\n" +
				"TASK WriteStub AFTER PopulateTable WRITE CSV 'stub.csv' FROM TABLE 'test.importtarget' END TASK\n" +
				"TASK ReadStub AFTER WriteStub READ CSV 'stub.csv' INTO TABLE 'test.importtarget'\n" +
				"	PREFIX WITH 'INSERT INTO test.importtarget VALUES (456, ''doris dog'');'\n" +
				"END TASK\n" +
				"TASK ShowRows AFTER ReadStub FOR number, word FROM SQL SELECT * FROM test.importtarget ORDER BY number\n" +
				"	TASK ShowRow LOG FORMAT(number, 'd') + ',' + word END TASK\n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, assureTestTablesExist);
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		Analyzer.Record record;
		String fields[];

		record = recordIterator.next();
		fields = record.message.split(",");
		assertEquals("321", fields[0]);
		assertEquals("splunge", fields[1]);

		record = recordIterator.next();
		fields = record.message.split(",");
		assertEquals("321", fields[0]);
		assertEquals("splunge", fields[1]);

		record = recordIterator.next();
		fields = record.message.split(",");
		assertEquals("456", fields[0]);
		assertEquals("doris dog", fields[1]);

		recordIterator.next();
		assertFalse(recordIterator.hasNext());
	}

	public void testReadBadSyntax() throws Exception {
		String script;
		String message;

		script = "TASK READ CSV 'import no header.csv' WITH NO HEADERS INTO TABLE 'test.importtarget' END TASK \n";
		message = "At line 1: READ INTO TABLE requires column headers";
		assertBadSyntax(script, message);

		script = "TASK READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 3, 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n";
		message = "At line 1: Would attempt to read column beyond bounds of record at position 3";
		assertBadSyntax(script, message);

		script = "TASK READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 'Arbitrary','Wrong Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n";
		message = "At line 1: Would attempt to read column with non-existing header \"Wrong Stuff\"";
		assertBadSyntax(script, message);
	}
}
