/*
 * Copyright (c) 2017, 2018, Ronald DeSantis
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
				"VARIABLES number INT, word VARCHAR, text VARCHAR, true BIT, false BIT END VARIABLES\n" +
				"TASK SetBits SET true = 1, false = 0 END TASK\n" +
				"TASK WriteCsv \n" +
				"	WRITE CSV 'valuesTest.csv' NOQUOTES \n" +
				"	HEADERS 'A string', 'An integer', 'A date' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5)), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31)) \n" +
				"END TASK\n" +
				"TASK WriteXlsx AFTER SetBits\n" +
				"	WRITE XLSX 'valuesTest.xlsx' 'The Sheet' \n" +
				"	HEADERS 'Stringy', 'Integral Value', 'Important Date', 'A Bit' \n" +
				"	FROM VALUES ('first', 1, DATEFROMPARTS(2017, 3, 5), true), ('SECOND Row', 22, DATEFROMPARTS(1999, 12, 31), false) \n" +
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
				"	TASK AFTER\n" +
				"		WRITE HTML text HEADERS 'nothing' FROM VALUES (NULL) \n" +
				"	END TASK \n" +
				"END TASK\n" +
				"TASK WriteStyledXls \n" +
				"	DO \n" +
				"	TASK \n" +
				"		WRITE XLSX 'styledTest.xlsx' 'Borders' STYLED \n" +
				"		TABLE STYLE 'border-collapse:collapse' \n" +
				"		NO HEADERS FROM VALUES \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: thin double\">thin double', NULL, '<td style=\"border: medium double\">medium double', NULL, '<td style=\"border: thick double\">thick double', NULL, NULL), \n" +
				"		(NULL, NULL, '<td style=\"border: thin solid\">thin solid', NULL, '<td style=\"border: medium solid\">medium solid', NULL, '<td style=\"border: thick solid\">thick solid', NULL), \n" +
				"		(NULL, '<td style=\"border: thin dashed\">thin dashed', NULL, '<td style=\"border: medium dashed\">medium dashed', NULL, '<td style=\"border: thick dashed\">thick dashed', NULL, NULL), \n" +
				"		(NULL, NULL, '<td style=\"border: thin dotted\">thin dotted', NULL, '<td style=\"border: medium dotted\">medium dotted', NULL, '<td style=\"border: thick dotted\">thick dotted', NULL), \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		(NULL, '<td style=\"border:double\">double','<td style=\"border:solid\">solid', \n" +
				"		 '<td style=\"border:dashed\">dashed','<td style=\"border:dotted\">dotted','<td style=\"border:hidden\">hidden',NULL,NULL), \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		(NULL, '<td style=\"border:hidden\">hidden','<td style=\"border:dotted\">dotted', \n" +
				"		 '<td style=\"border:dashed\">dashed','<td style=\"border:solid\">solid','<td style=\"border:double\">double',NULL,NULL), \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		('<td style=\"border: hidden\">hidden','<td style=\"border:double\">double','<td style=\"border:hidden\">hidden','<td style=\"border:solid\">solid', \n" +
				"		 '<td style=\"border: hidden\">hidden','<td style=\"border:dashed\">dashed','<td style=\"border:hidden\">hidden', '<td style=\"border:dotted\">dotted'), \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: double\">double', NULL, '<td style=\"border: double\">double', NULL, '<td style=\"border: hidden\">hidden', NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: solid\">solid', NULL, '<td style=\"border: hidden\">hidden', NULL, '<td style=\"border: dotted\">dotted', NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: dashed\">dashed', NULL, '<td style=\"border: solid\">solid', NULL, '<td style=\"border: dashed\">dashed', NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: dotted\">dotted', NULL, '<td style=\"border: hidden\">hidden', NULL, '<td style=\"border: solid\">solid', NULL, NULL), \n" +
				"		(NULL, '<td style=\"border: hidden\">hidden', NULL, '<td style=\"border: dashed\">dashed', NULL, '<td style=\"border: double\">double', NULL, NULL), \n" +
				"		(NULL, NULL, NULL, '<td style=\"border: hidden\">hidden', NULL, '<td style=\"border: hidden\">hidden', NULL, NULL), \n" +
				"		(NULL, NULL, NULL, '<td style=\"border: dotted\">dotted', NULL, NULL, NULL, NULL), \n" +
				"		(NULL, NULL, NULL, '<td style=\"border: hidden\">hidden', NULL, NULL, NULL, NULL), \n" +
				"		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \n" +
				"		(NULL, '<td style=\"border:thin solid\">thin', '<td style=\"border:medium solid\">medium', '<td style=\"border:thick solid\">thick', \n" +
				"		 NULL,'<td style=\"border:thin solid\">thin', NULL, '<td style=\"border:thick solid\">thick'), \n" +
				"		(NULL, NULL, NULL, NULL, \n" +
				"		 NULL, '<td style=\"border:medium solid\">medium', NULL, '<td style=\"border:medium solid\">medium'), \n" +
				"		(NULL, '<td style=\"border:thick solid\">thick','<td style=\"border:medium solid\">medium','<td style=\"border:thin solid\">thin', \n" +
				"		 NULL,'<td style=\"border:thick solid\">thick',NULL,'<td style=\"border:thin solid\">thin') \n" +
				"	END TASK \n" +
				"	TASK AFTER \n" +
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
				"		('<tr style=\"background-color: #E6E6FA\"><td style=\"background-color: green\">', 'Lavender = #E6E6FA'), \n" +
				"		('<tr style=\"background-color: HotPink \">', 'HotPink '), \n" +
				"		('<tr style=\"background-color: #FF69B4\">', '#FF69B4'), \n" +
				"		(' ', NULL), \n" +
				"		('normal', 123), \n" +
				"		('The answer is', '<td style=\"border-top:thin solid;border-bottom:double\">42'), \n" +
				"		('normal', '1,23,456'), \n" +
				"		('normal', '1,234'), \n" +
				"		('normal', '1,234.5'), \n" +
				"		('normal', '1,234.567'), \n" +
				"		('normal', '1,234.56'), \n" +
				"		('The answer is', '<td style=\"border-top:thin solid;border-bottom:double\">56,789,012.34'), \n" +
				"		('normal', 456) \n" +
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

	public void testDebug() throws Exception {

		String processId = "debug";
		String script =
				"TASK \n" +
				"	WRITE XLSX 'debugStyled.xlsx' 'Body' STYLED \n" +
				"	TABLE STYLE 'border-collapse:collapse;border:solid red' \n" +
				"	HEAD STYLE 'background-color:LightBlue;font-weight:bold' BODY STYLE 'border:solid blue' CELL STYLE 'border: thin solid red' \n" +
				"	HEADERS 'First', '<th style=\"color:red;border :  thick AquaMarine \">Second' FROM VALUES \n" +
				"	('<td style=\"font-style:italic;border-bottom: medium dashed purple;text-decoration:line-through\">cell A1', 'cell B1'), \n" +
				"	('<tr style=\"background-color:LemonChiffon\">cell A2', '<td style=\"border:double\">cell B2') \n" +
				"END TASK \n" +
				"TASK AFTER \n" +
				"	APPEND XLSX 'debugStyled.xlsx' 'Body' FROM VALUES \n" +
				"	('cell A3', '<td style=\"font-weight:bold;color:red;border:thick solid blue\">cell B3'), \n" +
				"	('<tr style=\"background-color:Green\">cell A4', '<td style=\"border-bottom:medium double\">cell B4') \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}

	public void testNullFileName() throws Exception {

		String script =
				"TASK WriteCsv \n" +
				"	WRITE CSV NULL NOQUOTES \n" +
				"	FROM VALUES ('first', 1), ('SECOND Row', 22) \n" +
				"END TASK\n" +
				"";

		assertScriptFails("WriteNullFileName", script, "WRITECSV", "File path expression evaluates to NULL");
	}

	public void testNullSheetName() throws Exception {

		String script =
				"TASK WriteXlsx \n" +
				"	WRITE XLSX 'dontcreate.xlsx' NULL STYLED \n" +
				"	FROM VALUES ('first', 1), ('SECOND Row', 22) \n" +
				"END TASK \n" +
				"";

		assertScriptFails("WriteNullSheetName", script, "WRITEXLSX", "Sheet name expression evaluates to NULL");
	}
}
