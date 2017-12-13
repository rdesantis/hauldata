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
				"	DO \n" +
				"	TASK \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Mixed' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse;border:solid red' \n" +
				"		HEAD STYLE 'background-color:LightBlue;font-weight:bold' BODY STYLE 'border:solid blue' CELL STYLE 'border: thin solid red' \n" +
				"		HEADERS 'First', '<th style=\"color:red;border :  thick AquaMarine \">Second' FROM VALUES \n" +
				"		('<td style=\"font-style:italic;border-bottom: medium dashed purple;text-decoration:line-through\">cell A1', 'cell B1'), \n" +
				"		('<tr style=\"background-color:LemonChiffon\">cell A2', '<td style=\"border:double\">cell B2') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Table' \n" +
				"		TABLE STYLE 'border-collapse:collapse;border:solid red' \n" +
				"		HEADERS 'First', 'Second' FROM VALUES \n" +
				"		('cell A1', 'cell B1'), \n" +
				"		('cell A2', 'cell B2') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Head' \n" +
				"		TABLE STYLE 'border-collapse:collapse' \n" +
				"		HEAD STYLE 'border:dotted lime' \n" +
				"		HEADERS 'First', 'Second' FROM VALUES \n" +
				"		('cell A1', 'cell B1'), \n" +
				"		('cell A2', 'cell B2') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Body' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse' \n" +
				"		BODY STYLE 'border:dashed orange;background-color:yellow' \n" +
				"		HEADERS 'First', '<th style=\"color:red;border : thick solid AquaMarine;background-color:LightBlue;font-weight:bold \">Second' FROM VALUES \n" +
				"		('cell A1', 'cell B1'), \n" +
				"		('cell A2', 'cell B2') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Head and Body' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse' \n" +
				"		HEAD STYLE 'border:dotted lime' \n" +
				"		BODY STYLE 'border:dashed orange;background-color:yellow' \n" +
				"		HEADERS 'First', '<th style=\"color:red;border : thick solid AquaMarine;background-color:LightBlue;font-weight:bold \">Second', 'Third' FROM VALUES \n" +
				"		('cell A1', 'cell B1', 'cell C1'), \n" +
				"		('cell A2', 'cell B2', 'cell C2') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'px' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse' \n" +
				"		NO HEADERS FROM VALUES \n" +
				"		('<td style=\"border: 0px solid\">0px', NULL, '<td style=\"border: 1px solid\">1px', NULL, '<td style=\"border: 2px solid\">2px'), \n" +
				"		(NULL, '<td style=\"border: 3px solid\">3px', NULL, '<td style=\"border: 4px solid\">4px', NULL), \n" +
				"		('<td style=\"border: 5px solid\">5px', NULL, '<td style=\"border: 10px solid\">10px', NULL, '<td style=\"border: 99999px solid\">99999px') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Answer' STYLED \n" +
				"		NO HEADERS FROM VALUES \n" +
				"		('The answer is', '<td style=\"border-bottom:double\">42'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">-42'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">1234567890'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">-1234567890'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">12345678901234567890'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">123.45'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">-123.45'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">1.50'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">3.00'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">1.23456'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">3.14159265358979323846264'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">042'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">0'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">-0'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">0.1'), \n" +
				"		('No, it''s ', '<td style=\"border-bottom:double\">-0.1'), \n" +
				"		(NULL, NULL), \n" +
				"		('<tr style=\"background-color: Beige\">', 123), \n" +
				"		('<tr style=\"background-color: Beige\">', 4567), \n" +
				"		('<tr style=\"background-color: DimGrey\">', DATEADD(DAY, 0, '12/31/1999')), \n" +
				"		('<tr style=\"background-color: DimGrey\">', DATEADD(DAY, 0, '12/9/2017')), \n" +
				"		('<tr style=\"background-color: Fuchsia\">', DATEADD(DAY, 0, '12/9/2017 1:23')), \n" +
				"		('<tr style=\"background-color: Fuchsia\">', 'Fuschsia'), \n" +
				"		('<tr style=\"background-color: #FF00FF\">', '#FF00FF'), \n" +
				"		('<tr style=\"background-color: #E6E6FA\">', 'Lavender = #E6E6FA'), \n" +
				"		('<tr style=\"background-color: HotPink \">', 'HotPink '), \n" +
				"		('<tr style=\"background-color: #FF69B4\">', '#FF69B4') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Aligned' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse;border:solid;text-align:center' \n" +
				"		HEADERS 'First', 'Second', 'Third' FROM VALUES \n" +
				"		('cell A1 elongated', 'cell B1', 'cell C1 elongated'), \n" +
				"		('cell A2', 'cell B2 elongated', 'cell C1'), \n" +
				"		('aaa', 'bbb', 'ccc'), \n" +
				"		('<td style=\"text-align:left\">left', 'default', '<td style=\"text-align:right\">right') \n" +
				"	END TASK \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
