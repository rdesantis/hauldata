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

import java.io.StringReader;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.DbProcess;

public class ConnectTaskTest extends TaskTest {

	public ConnectTaskTest(String name) {
		super(name);
	}

	public void testNoSuchConnection() throws Exception {

		String script = 
				"CONNECTIONS one DATABASE, two FTP, three EMAIL END CONNECTIONS \n" +
				"TASK CONNECT four TO 'whatever value' END TASK \n" +
				"";

		DbProcess process = null;
		String message = null;

		try {
			process = DbProcess.parse(new StringReader(script));
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}

		assertNull(process);
		assertEquals("At line 2: Connection name not declared: FOUR", message);
	}

	public void testConnect() throws Exception {

		String processId = "ConnectTest";
		String script =
				"VARIABLES server VARCHAR END VARIABLES \n" +
				"CONNECTIONS one DATABASE, two FTP, three EMAIL END CONNECTIONS \n" +
				"TASK WithDefault CONNECT one TO DEFAULT WITH 'url kdbc:yoursql://localhost/whatever Other \"thing within 2 (yes 2) quotes\"' END TASK \n" +
				"TASK SetServer SET server = 'some:wackadoodie.thing' END TASK \n" +
				"TASK NoDefault AFTER SetServer CONNECT two TO 'Server ' + server END TASK \n" +
				"TASK OnlyDefault CONNECT three DEFAULT END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null); 
	}

	public void testConnectionPropertiesNotSet() throws Exception {
		
		String processId = "ConnectNoPropertiesTest";
		String script =
				"CONNECTIONS db DATABASE END CONNECTIONS \n" +
				"TASK BadWrite WRITE CSV 'nothing' FROM db PROCEDURE 'whatever'  END TASK \n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null, false);

		Analyzer.RecordIterator iterator = analyzer.recordIterator(processId, "BADWRITE");
		Analyzer.Record record;

		record = iterator.next();
		assertEquals("Database connection properties are not defined", record.message);

		record = iterator.next();
		assertEquals(Task.failMessage, record.message);

		assertFalse(iterator.hasNext());
	}
}
