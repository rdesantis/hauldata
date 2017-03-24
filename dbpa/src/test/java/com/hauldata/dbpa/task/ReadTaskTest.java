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

public class ReadTaskTest extends TaskTest {

	public ReadTaskTest(String name) {
		super(name);
	}

	public void testRead() throws Exception {

		String processId = "ReadTest";
		String script =
				"TASK RUN SQL TRUNCATE TABLE test.importtarget; DELETE FROM test.things WHERE name = 'Content'; END TASK \n" +

				"TASK ReadUTF8BOM   AFTER READ CSV 'utf-8 no bom.csv' HEADERS 'the_geom', 'BoroCode', 'BoroName', 'Shape_Leng', 'Shape_Area' COLUMNS 2, 3 INTO TABLE 'test.importtarget' END TASK \n" +
				"TASK ReadUTF8NoBOM AFTER READ CSV 'utf-8 bom.csv' HEADERS 'Name', 'Value' INTO SQL INSERT INTO test.things (name, description) VALUES (?,?) END TASK \n" +

				"TASK ReadCsv1 AFTER          COMPLETES READ CSV 'import no header.csv' WITH NO HEADERS INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
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

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		assertNextTaskFailed(recordIterator, "FAILCSV4", "The file has wrong number of columns for the requested operation");
		assertNextTaskFailed(recordIterator, "FAILCSV5", "Expected column header 'Wrong', found 'Numero'");
		assertNextTaskFailed(recordIterator, "FAILCSV6", "Expected column header 'Not Enough Headers', found 'Numero'");
		assertNextTaskFailed(recordIterator, "FAILCSV7", "Expected column header 'Too', found 'Numero'");
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

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
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

	public void testReadXlsx() throws Exception {

		String target = "../../../../target/test/resources/data/";

		String processId = "ReadXlsxTest";
		String script =
				"VARIABLES things_count INTEGER, things_validate_count INTEGER, lotsofdata_count INTEGER, lotsofdata_validate_count INTEGER, threecolumns_validate_count INTEGER END VARIABLES \n" +
				"TASK GetThingsCount UPDATE things_count FROM SQL SELECT COUNT(*) FROM test.things END TASK \n" +
				"TASK GetLotsOfDataCount UPDATE lotsofdata_count FROM SQL SELECT COUNT(*) FROM test.lotsofdata END TASK \n" +
				"TASK AFTER GetThingsCount AND GetLotsOfDataCount RUN SQL UPDATE test.things SET size = 0 WHERE size IS NULL END TASK \n" +
				"TASK WriteFirst AFTER WRITE XLSX 'readable.xlsx' 'First' SHEET FROM TABLE 'test.things' END TASK\n" +
				"TASK WriteSecond AFTER WRITE XLSX 'readable.xlsx' 'Second' SHEET WITH HEADERS 'Identifier', 'What It Is' FROM TABLE 'test.lotsofdata' END TASK\n" +
				"TASK WriteThird AFTER WRITE XLSX 'readable.xlsx' 'Third' SHEET WITH HEADERS 'Hunky', 'Dory' FROM VALUES (NULL, NULL) END TASK\n" +
				"TASK WriteFourth AFTER WRITE XLSX 'readable.xlsx' 'Fourth' SHEET WITH HEADERS 'Humpty', 'Dumpty', 'Wall' FROM VALUES (1, 2, 3) END TASK\n" +
				"TASK InsertFifth AFTER RUN SQL\n" +
				"	USE test; TRUNCATE TABLE threecolumns; TRUNCATE TABLE threecolumns_validate;\n" +
				"	INSERT INTO threecolumns VALUES ('first', 1, NULL), ('second', 2, '1999-12-31'), ('third', 3, '2017-03-24 00:18:59');\n" +
				"END TASK\n" +
				"TASK WriteFifth AFTER WRITE XLSX 'readable.xlsx' 'Fifth' SHEET FROM TABLE 'test.threecolumns' END TASK\n" +
				"TASK AFTER RUN SQL TRUNCATE TABLE test.things_validate; TRUNCATE TABLE test.lotsofdata_validate; END TASK\n" +
				"TASK ReadFirst AFTER READ XLSX '" + target + "readable.xlsx' 'First' SHEET INTO TABLE 'test.things_validate' END TASK\n" +
				"TASK ReadSecond AFTER READ XLSX '" + target + "readable.xlsx' 'Second' SHEET WITH HEADERS 'Identifier', 'What It Is' INTO TABLE 'test.lotsofdata_validate' END TASK\n" +
				"TASK GetThingsValidateCount AFTER UPDATE things_validate_count FROM SQL \n" +
				"	SELECT COUNT(*) FROM test.things tt INNER JOIN test.things_validate ttv \n" +
				"	ON ttv.name = tt.name WHERE ttv.description = tt.description AND ttv.size = tt.size \n" +
				"END TASK \n" +
				"TASK FailThings AFTER IF things_validate_count <> things_count FAIL 'Wrong COUNT for things_validate: ' + FORMAT(things_validate_count, 'd') END TASK\n" +
				"TASK GetLotsOfDataValidateCount AFTER UPDATE lotsofdata_validate_count FROM SQL \n" +
				"	SELECT COUNT(*) FROM test.lotsofdata tl INNER JOIN test.lotsofdata_validate tlv \n" +
				"	ON tlv.data_id = tl.data_id WHERE tlv.description = tl.description \n" +
				"END TASK \n" +
				"TASK FailLotsOfData AFTER IF lotsofdata_validate_count <> lotsofdata_count FAIL 'Wrong COUNT for lotsofdata_validate' + FORMAT(lotsofdata_validate_count, 'd') END TASK\n" +
				"TASK FailThird AFTER READ XLSX '" + target + "readable.xlsx' 'Third' SHEET WITH HEADERS 'Identifier', 'What It Is' INTO TABLE 'test.lotsofdata_validate' END TASK\n" +
				"TASK AFTER FailThird SUCCEEDS FAIL 'Wrong header NOT correctly detected' END TASK \n" +
				"TASK AFTER FailThird FAILS LOG 'Wrong header correctly detected' END TASK\n" +
				"TASK FailFourth AFTER READ XLSX '" + target + "readable.xlsx' 'Fourth' SHEET WITH HEADERS 'Humpty', 'Dumpty' INTO TABLE 'test.lotsofdata_validate' END TASK\n" +
				"TASK AFTER FailFourth SUCCEEDS FAIL 'Too many headers NOT correctly detected' END TASK \n" +
				"TASK AFTER FailFourth FAILS LOG 'Too many headers correctly detected' END TASK\n" +
				"TASK ReadFifth AFTER READ XLSX '" + target + "readable.xlsx' 'Fifth' SHEET INTO TABLE 'test.threecolumns_validate' END TASK\n" +
				"TASK GetThreeColumnsValidateCount AFTER UPDATE threecolumns_validate_count FROM SQL \n" +
				"	SELECT COUNT(*) FROM test.threecolumns tc INNER JOIN test.threecolumns_validate tcv \n" +
				"	ON tcv.an_integer = tc.an_integer WHERE tcv.a_string = tc.a_string AND (tc.a_datetime IS NULL OR tcv.a_datetime = tc.a_datetime) \n" +
				"END TASK \n" +
				"TASK FailThreeColumns AFTER IF threecolumns_validate_count <> 3 FAIL 'Wrong COUNT for threecolumns_validate: ' + FORMAT(threecolumns_validate_count, 'd') END TASK\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
