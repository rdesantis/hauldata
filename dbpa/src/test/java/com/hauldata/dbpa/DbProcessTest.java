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

package com.hauldata.dbpa;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.naming.NamingException;

import com.hauldata.dbpa.loader.Loader;
import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.ConsoleAppender;
import com.hauldata.dbpa.log.FileAppender;
import com.hauldata.dbpa.log.Logger;
import com.hauldata.dbpa.log.RootLogger;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.task.TaskTest;

public class DbProcessTest extends TaskTest {

	public DbProcessTest(String name) {
		super(name);
	}

	public void testEverything() throws Exception {

		// Set up context properties.

		DbProcessTestProperties testProps = new DbProcessTestPropertiesImpl();

		Properties connProps = testProps.getConnectionProperties();
		Properties mailProps = testProps.getMailProperties();
		Properties ftpProps = testProps.getFtpProperties();
		String dataPath = testProps.getDataPath();
		String logPath = testProps.getLogPath();

		// Create context and set up logging.

		Context context = new Context(connProps, mailProps, ftpProps, dataPath, new DummyLoader());

		RootLogger logger = new RootLogger("DBPATest", Logger.Level.info);
		logger.add(new ConsoleAppender());
		logger.add(new FileAppender(logPath + "\\" + "test %d{yyyy-MM-dd} at time %d{HH-mm-ss}.log", "Daily every 10 seconds"));
		context.logger = logger;

		// Make sure the test tables exist.

		DbProcessTestTables.assureExist(context);

		// Now ready to run scripts.

		String script;
///*
		script =
				"PARAMETERS increment INTEGER END PARAMETERS \n" +
				"VARIABLES future DATETIME, garbage VARCHAR, something VARCHAR, three INTEGER, \n" +
				"name VARCHAR, description VARCHAR, size INTEGER, report VARCHAR, report2 VARCHAR, import VARCHAR, csv2 VARCHAR, child VARCHAR, tsvName VARCHAR, \n" +
				"start DATETIME, finish DATETIME, when VARCHAR \n" +
				"END VARIABLES \n" +
				"TASK ForCsvLoop FOR size, description FROM CSV 'read file.csv' WITH HEADERS 'Numero', 'Parola' COLUMNS 1, 2 \n" +
					"TASK ForShow LOG 'number = ' + FORMAT(size, 'd') + ', text = \"' + description + '\"' END TASK \n" +
				"END TASK \n" +
				"TASK TxtExport WRITE TXT 'word.txt' NO HEADERS FROM SQL SELECT word FROM test.importtarget END TASK \n" +
				"TASK TxtImport AFTER TxtExport READ TXT 'inserter.sql' NO HEADERS INTO SQL INSERT INTO test.importtarget (word) VALUES (?) END TASK \n" +
				"TASK DateMath SET future = DATEADD(MONTH, increment, GETDATE()), something = 'Some Thing', three = 3 END TASK \n" +
				"TASK HelloWorld LOG 'Hello, world!' END TASK \n" +
				"TASK LogResult AFTER DateMath LOG 'Time ' + FORMAT(increment, 'd') + ' months from now is ' + FORMAT(future, 'M/d/yyyy h:mm:ss a') END TASK \n" +
				"TASK NeverRun AFTER DateMath FAILS OR HelloWorld FAILS LOG 'Should not see this.' END TASK \n" +
				"TASK AlsoNeverRun AFTER NeverRun LOG 'Should not see this either.' END TASK \n" +
				"TASK DbInsert AFTER DateMath AND HelloWorld AND LogResult RUN STATEMENT 'INSERT INTO test.things VALUES (''fred'', ''some guy named fred'', 42)' END TASK \n" +
				"TASK DbUpdate AFTER DateMath AND HelloWorld AND LogResult RUN STATEMENT 'UPDATE test.things SET size = size + 1 WHERE name = ''fred''' END TASK \n" +
				"TASK TestISNULL LOG 'ISNULL(garbage, ''NULL'') = ' + ISNULL(garbage, 'NULL') END TASK \n" +
				"TASK FailDueToNotSet LOG 'garbage = ' + garbage END TASK \n" +
				"TASK DbParam AFTER DbInsert FAILS AND DbUpdate SUCCEEDS RUN SQL SUBSTITUTING something, three INSERT INTO test.things VALUES (?, 'whatever', ?) END TASK \n" +
				"TASK DbUpdateVars AFTER DbParam COMPLETES UPDATE name, description, size FROM STATEMENT 'SELECT * FROM test.things WHERE name = ''fred''' END TASK \n" +
				"TASK ShowUpdatedVars AFTER DbUpdateVars LOG 'freds columns are \"' + name + '\", \"' + description + '\", ' + FORMAT(size, 'd') END TASK \n" +
				"TASK GetEthel AFTER ShowUpdatedVars UPDATE size, description FROM SQL SUBSTITUTING 'ethel' INTO SELECT size, description FROM test.things WHERE name = ? END TASK \n" +
				"TASK ShowEthel AFTER GetEthel LOG 'Ethel is a ' + description + ', size ' + FORMAT(size, 'd') END TASK \n" +
				"TASK NameCsv AFTER ShowEthel SET report = 'test file.csv', report2 = 'another test file.xlsx' END TASK \n" +
				"TASK CreateCsv AFTER NameCsv CREATE CSV report WITH 'Who', 'More About Them', 'How Big' END TASK \n" +
				"TASK WriteCsv AFTER CreateCsv APPEND CSV report FROM SQL SELECT * FROM test.things END TASK\n" +
				"TASK CreateXlsx AFTER WriteCsv CREATE XLSX report2 'First' SHEET WITH 'Who Else', 'The Same About Them', 'Some Number' END TASK \n" +
				"TASK WriteXlsx AFTER CreateXlsx APPEND XLSX report2 'First' SHEET FROM STATEMENT 'SELECT * FROM test.things' END TASK\n" +
				"TASK CreateXlsx2 AFTER WriteXlsx CREATE XLSX report2 'Big Data' SHEET WITH 'ID', 'Description' END TASK \n" +
				"TASK WriteXlsx2 AFTER CreateXlsx2 APPEND XLSX report2 'Big Data' SQL SELECT * FROM test.lotsofdata END TASK \n" +
				"TASK IdCsv AFTER NameCsv SET import = 'import file.csv' END TASK \n" +
				"TASK OpenCsv AFTER IdCsv OPEN CSV import WITH 'number', 'word' END TASK \n" +
				"TASK LoadCsv AFTER OpenCsv LOAD CSV import INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"TASK LoadCsv2 AFTER LoadCsv LOAD CSV import INTO STATEMENT 'INSERT INTO test.importtarget (number, word) VALUES (?, ?)' END TASK \n" +
				"TASK OpenNonExistent AFTER LoadCsv2 FAILS OPEN CSV 'garbage.crap' END TASK\n" +
				"TASK ReadCsv AFTER LoadCsv READ CSV 'read file.csv' WITH HEADERS 'Numero','Parola' INTO STATEMENT 'INSERT INTO test.importtarget (number, word) VALUES (?, ?)' END TASK \n" +
				"TASK NameCsv2 AFTER OpenNonExistent COMPLETES SET csv2 = 'no header.csv' END TASK \n" +
				"TASK CreateCsv2 AFTER NameCsv2 CREATE CSV csv2 WITH NO HEADERS END TASK \n" +
				"TASK WriteCsv2 AFTER CreateCsv2 APPEND CSV csv2 FROM SQL SELECT * FROM test.importtarget END TASK \n" +
/*mail*///		"TASK EmailCsv2 AFTER WriteCsv2 EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'From DBPA' ATTACH csv2 END TASK \n" +
//TODO: Remove hard-coded driver letter!
"TASK NameCsv3 AFTER OpenNonExistent COMPLETES SET child = 'D:\\Temp\\child\\child file.csv' END TASK \n" +
				"TASK CreateCsv3 AFTER NameCsv3 CREATE CSV child END TASK \n" +
				"TASK WriteCsv3 AFTER CreateCsv3 APPEND CSV child FROM SQL SELECT * FROM test.things END TASK\n" +
				"TASK NameTsv AFTER OpenNonExistent COMPLETES SET tsvName = 'tabbed.tsv' END TASK \n" +
				"TASK CreateTsv AFTER NameTsv CREATE TSV tsvName END TASK \n" +
				"TASK WriteTsv AFTER CreateTsv APPEND TSV tsvName FROM SQL SELECT * FROM test.things END TASK\n" +
				"TASK AllWritten AFTER WriteCsv AND WriteXlsx AND WriteXlsx2 AND WriteCsv2 AND WriteCsv3 AND WriteTsv GO END TASK \n" +
				"TASK ZipAll AFTER AllWritten ZIP report, report2, csv2, child, tsvName TO 'steaming pile.zip' END TASK \n" +
				"TASK NewWrite WRITE CSV 'written.csv' FROM SQL SELECT * FROM test.things END TASK \n" +
				"TASK NewWriteHeaders WRITE CSV 'written headers.csv' HEADERS 'First', 'Second', 'Turd' FROM STATEMENT 'SELECT * FROM test.things' END TASK \n" +
				"TASK WriteMetadataHeaders WRITE CSV 'metadata headers.csv' FROM STATEMENT 'SELECT * FROM test.things' END TASK \n" +
				"TASK WriteTable WRITE CSV 'table explicit headers.csv' WITH HEADERS 'First', 'Second', 'Turd' FROM TABLE 'test.things' END TASK \n" +
				"TASK WriteTableMetadataHeaders WRITE CSV 'table metadata headers.csv' WITH HEADERS FROM TABLE 'test.things' END TASK \n" +
				"TASK NewWriteXlsx AFTER WriteXlsx2 WRITE XLSX 'written headers.xlsx' 'only sheet' WITH 'xFIRSTx', 'xSECONDx', 'xTHIRDx' FROM SQL SELECT * FROM test.things END TASK \n" +
				"TASK NewWriteXlsx2 AFTER NewWriteXlsx WRITE XLSX 'written headers.xlsx' 'other sheet' WITH HEADERS FROM STATEMENT 'SELECT * FROM test.things' END TASK \n" +
				"TASK NewWriteXlsx3 AFTER NewWriteXlsx2 WRITE XLSX 'written headers.xlsx' 'yet another sheet' FROM SQL SELECT * FROM test.things END TASK \n" +
				"TASK NewWriteXlsx4 AFTER NewWriteXlsx3 WRITE XLSX 'written headers.xlsx' 'final sheet' NO HEADERS FROM SQL SELECT * FROM test.things END TASK \n" +
				"TASK Appender APPEND CSV 'append me.csv' FROM SQL SELECT * FROM test.things END TASK \n" +
				"TASK UnZipper UNZIP 'sql200n.zip' TO 'trash' END TASK \n" +
				"TASK Deleter DELETE 'final target.csv', 'target.csv' END TASK \n" +
				"TASK Copier AFTER Appender SUCCEEDS AND Deleter COMPLETES COPY 'append me.csv' TO 'target.csv' END TASK \n" +
				"TASK Renamer AFTER Copier SUCCEEDS AND Deleter COMPLETES RENAME 'target.csv' TO 'final target.csv' END TASK \n" +
				"TASK Nester AFTER UnZipper PROCESS 'whatever' WITH 'fred' + ' beans', 1 + 2 END TASK \n" +
				"TASK SkipMe AFTER ZipAll IF csv2 <> 'no header.csv' FAIL 'This should not have been logged.' END TASK \n" +
				"TASK OuterDo AFTER SkipMe DO TASK Do1 LOG 'In Do1' END TASK TASK Do2 AFTER Do1 LOG 'In Do2' END TASK TASK Nester LOG 'In nested Nester' END TASK END TASK \n" +
				"TASK LoopDeeDo AFTER OuterDo DO WHILE size <= 99999 \n" +
					"TASK Looper LOG 'Size is ' + FORMAT(size, 'd') END TASK \n" +
					"TASK Setter AFTER Looper SET size = size * 10 END TASK \n" +
				"END TASK \n" +
				"TASK ForLoop AFTER LoopDeeDo FOR name, description, size FROM SQL SELECT * FROM test.things \n" +
					"TASK ForShow LOG 'name = ' + name + ', description = ' + description + ', size = ' + FORMAT(size, 'd') END TASK \n" +
					"TASK Breaker AFTER ForShow IF size = 55555 FAIL 'This should break the FOR loop' END TASK \n" +
				"END TASK \n" +
				"TASK ShowDateFromParts AFTER ForLoop COMPLETES LOG '2/29/2016 from parts = ' + FORMAT(DATEFROMPARTS(2016, 2, 29), 'M/d/yyyy') END TASK \n" +
				"TASK MkDir AFTER ShowDateFromParts MAKE DIRECTORY 'where this goes' END TASK \n" +
				"TASK RunScript AFTER MkDir COMPLETES RUN SCRIPT 'inserter.sql' END TASK \n" +
//TODO: Remove hard-coded driver letter!
				"TASK ZipWildcards AFTER RunScript ZIP 'D:\\code\\dbpa\\*.properties', 'process\\*.*' TO 'wildcarded.zip' END TASK \n" +
				"TASK CopyWildcards AFTER ZipWildcards COPY 'written*.*', '*.sql' TO 'trash' END TASK \n" +
				"TASK DeleteWildcards AFTER UnZipper AND CopyWildcards DELETE 'trash\\*Framework*', 'trash\\5CD2-11-Schemata-2006-01.pdf' END TASK \n" +
				"TASK Async1 AFTER DeleteWildcards PROCESS ASYNC 'asyncproc1' WITH 'my butt', 1 END TASK \n" +
				"TASK Async2 AFTER Async1 PROCESS ASYNCHRONOUSLY 'asyncproc2' WITH 'your butt', 2 END TASK \n" +
				"TASK AsyncDone AFTER Async2 LOG 'After sequential async tasks' END TASK \n" +
				"TASK AsyncLoop AFTER AsyncDone DO WHILE size > 10 \n" +
					"TASK Looper PROCESS ASYNC 'asyncLoop' WITH 'thing', size END TASK \n" +
					"TASK Setter AFTER Looper SET size = size / 10 END TASK \n" +
				"END TASK \n" +
				"TASK SetRange AFTER AsyncLoop SET \n" +
					"start = DATEADD(SECOND, 2, GETDATE()), \n" +
					"finish = DATEADD(SECOND, 6, start), \n" +
					"when = 'TODAY NOW, TODAY EVERY 2 SECONDS FROM ''' + FORMAT(start, 'hh:mm:ss a') + ''' UNTIL ''' + FORMAT(finish, 'hh:mm:ss a') + '''' \n" +
				"END TASK \n" +
				"TASK ShowSchedule AFTER SetRange LOG when END TASK \n" +
				"TASK Scheduler3 AFTER ShowSchedule ON SCHEDULE when \n" +
					"TASK Schedulee3 LOG 'Recurring scheduled task' END TASK \n" +
				"END TASK \n" +
/*mail*///		"TASK EmailAgain AFTER DeleteWildcards EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'From DBPA again' BODY 'Re-using email session' END TASK \n" +
//				"TASK FailTheProcess AFTER ShowDateFromParts FAIL 'This should fail the process' END TASK \n" +
				"TASK AFTER Scheduler3 LOG 'Scheduled tasks are done' END TASK \n" +
				"TASK WaitforDelay3 AFTER Scheduler3 WAITFOR DELAY '0:0:3' END TASK \n" +
				"TASK WaitforTime3 AFTER Scheduler3 WAITFOR TIME FORMAT(DATEADD(SECOND, 3, GETDATE()), 'HH:mm:ss') END TASK \n" +
				"TASK LOG 'Early anonymous task' END TASK \n" +
				"";
//*/
/*
		LocalDateTime x;
		x = LocalDateTime.parse("2015-09-15 10:30", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
		x = LocalDateTime.parse("08/15/2015 10:30", DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm"));
		x = LocalDateTime.parse("8/5/2015 10:30", DateTimeFormatter.ofPattern("M/d/yyyy HH:mm"));
		x = LocalDateTime.parse("07/23/2015 10:30", DateTimeFormatter.ofPattern("M/d/yyyy HH:mm"));
		x = LocalDateTime.parse("07/23/2015 10:30", DateTimeFormatter.ofPattern("M/d/yyyy H:mm"));
		x = LocalDateTime.parse("07/24/2015 10:30", DateTimeFormatter.ofPattern("M/d/yyyy[ H:mm]"));
		x = LocalDate.parse("07/25/2015", DateTimeFormatter.ofPattern("M/d/yyyy[ H:mm]")).atStartOfDay();
		System.out.println(x);
*/

/*
		script =
				"VARIABLES \n" +
					"d1 DATETIME, d2 DATETIME, d3 DATETIME, \n" +
					"s1 VARCHAR, s2 VARCHAR, s3 VARCHAR, \n" + 
					"i1 INTEGER, i2 INTEGER, i3 INTEGER \n" +
				"END VARIABLES \n" +
				"TASK DateMath SET \n" +
					"d1 = '9/1/2015 9:30 PM', \n" + 
					"d2 = '2001-8-10 12:34:56', \n" +
					"d3 = '2015-09-05T00:40:26.090' \n" +
				"END TASK \n" +
				"TASK Echo AFTER DateMath LOG \n" +
					"'d1 = ' + FORMAT(d1,'yyyy-MM-dd HH:mm:ss') + ', d2 = ' + FORMAT(d2,'yyyy-MM-dd') + ', d3 = ' + FORMAT(d3,'M/d/yyyy h:mm:ss a') \n" +
				"END TASK \n" +
				"TASK Zipper AFTER Echo ZIP \n" +
					"FROM 'no header.csv', 'another test file.xlsx' TO 'no header.zip' \n" +
				"END TASK \n" +
				"TASK UnZipper UNZIP \n" +
					"FROM 'sql200n.zip' TO 'trash' \n" +
//					"FROM 'sql200n.zip' \n" +
				"END TASK \n" +
				"";
*/
/*
		script = 
				"VARIABLES import VARCHAR END VARIABLES \n" +
				"TASK IdCsv SET import = 'import file.csv' END TASK \n" +
				"TASK OpenCsv AFTER IdCsv OPEN CSV import WITH 'number', 'word' END TASK \n" +
				"TASK ReadCsv AFTER OpenCsv READ CSV import INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n" +
				"";
*/		
/*
		script = 
				"TASK Upload PUT 'test file.csv' '/root/staff/test' END TASK \n" +
				"TASK Upload2 PUT 'another test file.xlsx', 'written.csv' TO '/root/staff/test' END TASK \n" +
				"";
*/
/*
		script = 
				"TASK Email1               EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'Only body' BODY 'Only has this body' END TASK \n" +
				"TASK Email2 AFTER Email1  EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'Only attachment' ATTACH 'import file.csv' END TASK \n" +
				"TASK Email3 AFTER Email2  EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'Body and attachment' BODY 'Has this body and attachment' ATTACH 'import file.csv' END TASK \n" +
				"TASK Email4 AFTER Email3  EMAIL FROM 'rdesantis@cmtnyc.com' TO 'rdesantis@comcast.net' SUBJECT 'Neither body nor attachment' END TASK \n" +
				"";
*/

		String[] args = new String[] { "11" };

		runScript(new StringReader(script), args, context);

		context.close();
	}

	private static void runScript(Reader r, String[] args, Context context) {

		try {
			DbProcess process = DbProcess.parse(r);
			process.run(args, context);
		}
		catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}

	private static class DummyLoader implements Loader {

		@Override
		public DbProcess load(String name) throws IOException, NamingException {

			final String script =
					"PARAMETERS text VARCHAR, number INTEGER END PARAMETERS \n" +
					"VARIABLES dimension INT -- This is a comment. \n" +
					"END VARIABLES -- This is ALSO a comment \n" +
					"TASK Nested LOG 'Arguments are: \"' + text + '\", \"' + FORMAT(number, 'd') + '\"' END TASK \n" +
					"TASK UpdateFred UPDATE dimension FROM SQL SELECT size FROM test.things WHERE name = 'fred' END TASK \n" +
					"TASK Show AFTER UpdateFred LOG 'Fred''s size is ' + FORMAT(dimension, 'd') END TASK \n" +
					"-- very end";

			return DbProcess.parse(new StringReader(script));
		}
	}

	public void testNulls() throws Exception {

		String processId = "NullsTest";
		String script =
				"PARAMETERS increment INTEGER END PARAMETERS \n" +
				"VARIABLES \n" + 
					"future DATETIME, something VARCHAR, three INTEGER, \n" +
					"nullint INT, notnullint INT, \n" +
					"nullchar VARCHAR, notnullchar VARCHAR, \n" +
					"nulldate DATE, notnulldate DATE \n" +
				"END VARIABLES \n" +
				"TASK Assign SET \n" +
					"future = DATEADD(MONTH, increment, GETDATE()), something = 'Some Thing', three = 3, \n" +
					"notnullint = 1, \n" +
					"notnullchar = 'stuff', \n" +
					"notnulldate = GETDATE() \n" +
				"END TASK \n" +
				"TASK log1 AFTER Assign LOG 'Time ' + FORMAT(increment, 'd') + ' months from now is ' + FORMAT(future, 'M/d/yyyy h:mm:ss a') END TASK \n" +
				"TASK log2 AFTER log1 COMPLETES LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log3 AFTER log2 LOG 'notnullint IS ' + IIF(notnullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log4 AFTER log3 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log5 AFTER log4 LOG 'notnullchar IS ' + IIF(notnullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log6 AFTER log5 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log7 AFTER log6 LOG 'notnulldate IS ' + IIF(notnulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK AssignNull AFTER log7 SET \n" +
					"nullint = NULL, \n" +
					"nullchar = null, \n" +
					"nulldate = Null \n" +
				"END TASK \n" +
				"TASK log12 AFTER AssignNull LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log14 AFTER log12 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log16 AFTER log14 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK ComputeNull AFTER log16 SET \n" +
					"nullint = NULL + 1, \n" +
					"nullchar = 'fred' + null, \n" +
					"nulldate = DATEFROMPARTS(2016, 1, nullint) \n" +
				"END TASK \n" +
				"TASK log22 AFTER ComputeNull LOG 'nullint IS ' + IIF(nullint IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log24 AFTER log22 LOG 'nullchar IS ' + IIF(nullchar IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log26 AFTER log24 LOG 'nulldate IS ' + IIF(nulldate IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log32 AFTER log26 LOG 'nullint expression IS ' + IIF(DATEPART(YEAR, nulldate) IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log34 AFTER log32 LOG 'nullchar expression IS ' + IIF(FORMAT(nullint, 'd') IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK log36 AFTER log34 LOG 'nulldate expression IS ' + IIF(DATEADD(DAY, nullint, GETDATE()) IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK Nester AFTER log36 PROCESS 'whatever' WITH nullchar + 'x', 1 + 2 END TASK \n" +
				"";

		String nestedScriptName = "whatever";
		String nestedScript =
				"PARAMETERS parm1 VARCHAR, parm2 INT END PARAMETERS\n" +
				"TASK nest1 LOG 'nullchar IS ' + IIF(parm1 IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"TASK nest2 AFTER nest1 LOG 'notnullint IS ' + IIF(parm2 IS NULL, '', 'NOT ') + 'NULL' END TASK \n" +
				"";

		Map<String, String> nestedScripts = new HashMap<String, String>();
		nestedScripts.put(nestedScriptName, nestedScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, nestedScripts, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, Pattern.compile("LOG\\d+$"));
		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals("LOG1", record.taskId);
		assertEquals("Message evaluates to NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG1", record.taskId);
		assertEquals(Task.failMessage, record.message);

		record = recordIterator.next();
		assertEquals("LOG2", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG3", record.taskId);
		assertEquals("notnullint IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG4", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG5", record.taskId);
		assertEquals("notnullchar IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG6", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG7", record.taskId);
		assertEquals("notnulldate IS NOT NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG12", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG14", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG16", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG22", record.taskId);
		assertEquals("nullint IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG24", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG26", record.taskId);
		assertEquals("nulldate IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG32", record.taskId);
		assertEquals("nullint expression IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG34", record.taskId);
		assertEquals("nullchar expression IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("LOG36", record.taskId);
		assertEquals("nulldate expression IS NULL", record.message);
		
		assertFalse(recordIterator.hasNext());

		String nestedProcessId = processId + "." + nestedScriptName;

		recordIterator = analyzer.recordIterator(nestedProcessId, Pattern.compile("NEST\\d+$"));

		record = recordIterator.next();
		assertEquals("NEST1", record.taskId);
		assertEquals("nullchar IS NULL", record.message);

		record = recordIterator.next();
		assertEquals("NEST2", record.taskId);
		assertEquals("notnullint IS NOT NULL", record.message);

		assertFalse(recordIterator.hasNext());
	}

	public void testSchedule() throws Exception {

		String processId = "ScheduleTest";
		String script =
				"VARIABLES when VARCHAR, start DATETIME, finish DATETIME, ms_format VARCHAR, no_ms_format VARCHAR END VARIABLES \n" +
				"TASK Scheduler ON TODAY NOW \n" +
					"TASK Schedulee LOG 'Scheduled task' END TASK \n" +
				"END TASK \n" +
				"TASK SetSchedule SET when = 'TODAY NOW' END TASK \n" +
				"TASK Scheduler2 AFTER Scheduler AND SetSchedule ON SCHEDULE when \n" +
					"TASK Schedulee2 LOG 'Another scheduled task' END TASK \n" +
				"END TASK \n" +
				"TASK SetRange AFTER Scheduler2 SET \n" +
					"start = DATEADD(SECOND, 2, GETDATE()), \n" +
					"finish = DATEADD(SECOND, 6, start), \n" +
					"when = 'TODAY NOW, TODAY EVERY 2 SECONDS FROM ''' + FORMAT(start, 'HH:mm:ss') + ''' UNTIL ''' + FORMAT(finish, 'HH:mm:ss') + '''', \n" +
					"no_ms_format = 'uuuu-MM-dd''T''HH:mm:ss', \n" +
					"ms_format = no_ms_format + '.SSS' \n" +
				"END TASK \n" +
				"TASK ShowSchedule AFTER SetRange LOG when END TASK \n" +
				"TASK ShowTimes AFTER ShowSchedule LOG FORMAT(GETDATE(), ms_format) + ',' + FORMAT(start, no_ms_format) + ',' + FORMAT(finish, no_ms_format) END TASK \n" +
				"TASK Scheduler3 AFTER ShowTimes ON SCHEDULE when \n" +
					"TASK Schedulee3 LOG 'Recurring scheduled task' END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		int frequency = 2;
		int cycles = 3;

		final int fuzzMillis = 150;
		final float fuzzSeconds = (float)fuzzMillis / 1000f;

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator("ScheduleTest", "SHOWTIMES");
		Analyzer.Record record = recordIterator.next();

		LocalDateTime messageTime = record.datetime;
		String[] times = record.message.split(",");
		LocalDateTime getdateTime = LocalDateTime.parse(times[0]);
		LocalDateTime startTime = LocalDateTime.parse(times[1]);
		LocalDateTime finishTime = LocalDateTime.parse(times[2]);

		assertEquals(0f, secondsBetween(messageTime, getdateTime), fuzzSeconds);
		assertEquals((float)frequency, secondsBetween(getdateTime, startTime), 1f);

		recordIterator = analyzer.recordIterator("ScheduleTest", "SCHEDULER3.SCHEDULEE3");
		String recurMessage = "Recurring scheduled task";

		record = recordIterator.next();
		assertEquals(recurMessage, record.message);
		assertEquals(0f, secondsBetween(getdateTime, record.datetime), fuzzSeconds);

		record = recordIterator.next();
		assertEquals(recurMessage, record.message);
		assertEquals(0f, secondsBetween(startTime, record.datetime), fuzzSeconds);

		LocalDateTime previousTime = record.datetime;
		for (int cycle = 0; cycle < cycles; ++cycle) {

			record = recordIterator.next();
			assertEquals(recurMessage, record.message);
			assertEquals((float)frequency, secondsBetween(previousTime, record.datetime), fuzzSeconds);

			previousTime = record.datetime;
		}

		assertEquals(0f, secondsBetween(finishTime, record.datetime), fuzzSeconds);

		assertFalse(recordIterator.hasNext());
	}

	private static float secondsBetween(LocalDateTime earlier, LocalDateTime later) {
		return (float)ChronoUnit.MILLIS.between(earlier, later) / 1000f;
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
		assertSyntaxError(script, message);

		script = "TASK READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 3, 2 INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n";
		message = "At line 1: Would attempt to read column beyond bounds of record at position 3";
		assertSyntaxError(script, message);

		script = "TASK READ CSV 'import arbitrary header.csv' WITH HEADERS 'Arbitrary','Random Stuff' COLUMNS 'Arbitrary','Wrong Stuff' INTO SQL INSERT INTO test.importtarget (number, word) VALUES (?, ?) END TASK \n";
		message = "At line 1: Would attempt to read column with non-existing header \"Wrong Stuff\"";
		assertSyntaxError(script, message);
	}

	private void assertSyntaxError(String script, String expectedMessage) {

		DbProcess process = null;
		String message = null;

		try {
			process = DbProcess.parse(new StringReader(script));
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}

		assertNull(process);
		assertEquals(expectedMessage, message);
	}
}
