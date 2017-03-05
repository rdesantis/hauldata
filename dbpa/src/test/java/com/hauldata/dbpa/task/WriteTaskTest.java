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

import com.hauldata.dbpa.log.Logger.Level;

public class WriteTaskTest extends TaskTest {

	public WriteTaskTest(String name) {
		super(name);
	}

	public void testWriteValues() throws Exception {

		String processId = "WriteValuesTest";
		String script =
				"VARIABLES number INT, word VARCHAR END VARIABLES\n" +
				"TASK WriteCsv \n" +
				"	WRITE CSV 'valuesTest.csv' \n" +
				"	HEADERS 'A string', 'An integer', 'A date' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5)), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31)) \n" +
				"END TASK\n" +
				"TASK WriteXlsx \n" +
				"	WRITE XLSX 'valuesTest.xlsx' 'The Sheet' \n" +
				"	HEADERS 'Stringy', 'Integral Value', 'Important Date' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5)), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31)) \n" +
				"END TASK\n" +
				"TASK ReadCsv AFTER \n" +
				"	READ CSV '../../../../target/test/resources/data/valuesTest.csv' \n" +
				"	HEADERS 'A string', 'An integer', 'A date' \n" +
				"	INTO SQL INSERT INTO test.threecolumns (a_string, an_integer, a_datetime) VALUES (?,?, ?) \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
