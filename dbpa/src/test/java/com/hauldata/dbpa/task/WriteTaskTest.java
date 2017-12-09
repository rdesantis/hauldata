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

import com.hauldata.dbpa.DbProcessTestTables;
import com.hauldata.dbpa.log.Logger.Level;

public class WriteTaskTest extends TaskTest {

	public WriteTaskTest(String name) {
		super(name);
	}

	public void testWriteValues() throws Exception {

		String processId = "WriteValuesTest";
		String script =
				"VARIABLES number INT, word VARCHAR, text VARCHAR END VARIABLES\n" +
				"TASK WriteCsv \n" +
				"	WRITE CSV 'valuesTest.csv' NOQUOTES \n" +
				"	HEADERS 'A string', 'An integer', 'A date' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5)), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31)) \n" +
				"END TASK\n" +
				"TASK WriteXlsx \n" +
				"	WRITE XLSX 'valuesTest.xlsx' 'The Sheet' \n" +
				"	HEADERS 'Stringy', 'Integral Value', 'Important Date' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5)), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31)) \n" +
				"END TASK\n" +
				"TASK ReadCsv AFTER WriteCsv \n" +
				"	READ CSV '../../../../target/test/resources/data/valuesTest.csv' \n" +
				"	HEADERS 'A string', 'An integer', 'A date' \n" +
				"	INTO SQL INSERT INTO test.threecolumns (a_string, an_integer, a_datetime) VALUES (?,?, ?) \n" +
				"END TASK\n" +
				"TASK WriteCrLf \n" +
				"	WRITE CSV 'crlf.csv' NOQUOTES CRLF \n" +
				"	HEADERS 'Header CrLf' \n" +
				"	FROM VALUES ('line 1'), ('line 2') \n" +
				"END TASK\n" +
				"TASK WriteLf \n" +
				"	WRITE CSV 'lf.csv' LF NOQUOTES \n" +
				"	HEADERS 'Header Lf' \n" +
				"	FROM VALUES ('1st line'), ('2nd line') \n" +
				"END TASK\n" +
				"TASK WriteHtml \n" +
				"	DO \n" +
				"	TASK \n" +
				"		WRITE HTML text TABLE STYLE 'border-collapse:collapse;border:solid red' BODY STYLE 'border:solid blue' CELL STYLE 'border: 1px solid black' \n" +
				"		HEADERS 'First', 'Second' \n" +
				"		FROM VALUES ('<i>cell A1</i>', 'cell B1'), ('<tr style=\"background-color:#4CAF50\">cell A2', '<td style=\"border:double\">cell B2') \n" +
				"	END TASK \n" +
				"	TASK AFTER\n" +
				"		WRITE TXT 'html.txt' NO HEADERS FROM VALUES (text) \n" +
				"	END TASK \n" +
				"END TASK\n" +
				"TASK WriteStyledXls \n" +
				"	WRITE XLSX 'styledTest.xlsx' 'Styled' STYLED \n" +
				"	TABLE STYLE 'border-collapse:collapse;border:solid red' \n" +
				"	HEAD STYLE 'background-color:LightBlue;font-weight:bold' BODY STYLE 'border:solid blue' CELL STYLE 'border: thin solid red' \n" +
				"	HEADERS 'First', '<th style=\"color:red;border :  thick AquaMarine \">Second' FROM VALUES \n" +
				"	('<td style=\"font-style:italic;border-bottom: medium dashed purple;text-decoration:line-through\">cell A1', 'cell B1'), \n" +
				"	('<tr style=\"background-color:LemonChiffon\">cell A2', '<td style=\"border:double\">cell B2') \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
