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
import java.io.StringReader;
import java.util.Properties;

import javax.naming.NamingException;

import com.hauldata.dbpa.loader.Loader;
import com.hauldata.dbpa.log.ConsoleAppender;
import com.hauldata.dbpa.log.FileAppender;
import com.hauldata.dbpa.log.Logger;
import com.hauldata.dbpa.log.RootLogger;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.task.TaskTest;

public class DbProcessTest extends TaskTest {

	public DbProcessTest(String name) {
		super(name);
	}

	public void testEverything() throws Exception {

		// Set up context properties.

		DbProcessTestPropertiesBase testProps = new DbProcessTestProperties();

		Properties connProps = testProps.getConnectionProperties();
		Properties mailProps = testProps.getMailProperties();
		Properties ftpProps = testProps.getFtpProperties();
		Properties pathProps = testProps.getPathProperties();

		String logFileName = pathProps.getProperty("log", ".") + "/" + "test %d{yyyy-MM-dd} at time %d{HH-mm-ss}.log";
		String logRollover = "Daily every 10 seconds";

		// Create context and set up logging.

		Context context = new Context(connProps, mailProps, ftpProps, pathProps, new DummyLoader());

		RootLogger logger = new RootLogger("DBPATest");
		Logger.Level level = Logger.Level.info;
		logger.add(new ConsoleAppender(level));
		logger.add(new FileAppender(logFileName, logRollover, level));
		context.logger = logger;

		// Make sure the test tables exist.

		DbProcessTestTables.assureExist(context);

		// Now ready to run scripts.

		String script =
				"PARAMETERS increment INTEGER END PARAMETERS \n" +
				"VARIABLES future DATETIME, garbage VARCHAR, something VARCHAR, three INTEGER, \n" +
				"name VARCHAR, description VARCHAR, size INTEGER, report VARCHAR, report2 VARCHAR, import VARCHAR, csv2 VARCHAR, child VARCHAR, tsvName VARCHAR, \n" +
				"start DATETIME, finish DATETIME, occurs VARCHAR \n" +
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
				"TASK NameCsv3 AFTER OpenNonExistent COMPLETES SET child = 'child/child file.csv' END TASK \n" +
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
				"TASK UnZipper UNZIP 'sql200n.zip' TO '../../../../target/test/resources/data/trash' END TASK \n" +
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
				"TASK ZipWildcards AFTER RunScript ZIP '*.csv', '../../../../src/test/resources/process/*.*' TO 'wildcarded.zip' END TASK \n" +
				"TASK CopyWildcards AFTER ZipWildcards COPY 'written*.*', '*.txt' TO 'trash' END TASK \n" +
				"TASK DeleteWildcards AFTER UnZipper AND CopyWildcards DELETE 'trash/*Framework*', 'trash/5CD2-11-Schemata-2006-01.pdf' END TASK \n" +
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
					"occurs = 'TODAY NOW, TODAY EVERY 2 SECONDS FROM ''' + FORMAT(start, 'hh:mm:ss a') + ''' UNTIL ''' + FORMAT(finish, 'hh:mm:ss a') + '''' \n" +
				"END TASK \n" +
				"TASK ShowSchedule AFTER SetRange LOG occurs END TASK \n" +
				"TASK Scheduler3 AFTER ShowSchedule ON SCHEDULE occurs \n" +
					"TASK Schedulee3 LOG 'Recurring scheduled task' END TASK \n" +
				"END TASK \n" +
				"TASK AFTER Scheduler3 LOG 'Scheduled tasks are done' END TASK \n" +
				"TASK WaitforDelay3 AFTER Scheduler3 WAITFOR DELAY '0:0:3' END TASK \n" +
				"TASK WaitforTime3 AFTER Scheduler3 WAITFOR TIME FORMAT(DATEADD(SECOND, 3, GETDATE()), 'HH:mm:ss') END TASK \n" +
				"TASK LOG 'Early anonymous task' END TASK \n" +
				"TASK AFTER FailDueToNotSet FAILS GO END TASK \n " +
				"";

		String[] args = new String[] { "11" };

		DbProcess process = DbProcess.parse(new StringReader(script));

		process.run(args, context);

		context.close();
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
}
