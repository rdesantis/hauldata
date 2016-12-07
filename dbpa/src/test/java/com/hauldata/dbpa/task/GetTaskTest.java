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

import com.hauldata.dbpa.log.Logger.Level;

public class GetTaskTest extends TaskTest {

	public GetTaskTest(String name) {
		super(name);
	}

	public void testGet() throws Exception {

		String processId = "GetTest";
		String script =
				"VARIABLES failed INT END VARIABLES \n" +

				"CONNECTIONS myftp FTP END CONNECTIONS \n" +
				"TASK Download1 GET '/root/staff/test/test file.csv' TO '.' END TASK \n" +
				"TASK Download2 GET '/root/staff/test/another test file.xlsx', '/root/staff/test/written.csv' END TASK \n" +
				"TASK Connector CONNECT myftp TO DEFAULT WITH 'timeout 15000 ignore this' END TASK \n" +
				"TASK Download3 AFTER Connector GET FROM myftp '/root/staff/test/word.txt' TO 'child' END TASK \n" +

				"TASK AFTER Download1 FAILS OR Download2 FAILS OR Download3 FAILS SET failed = 1 END TASK \n" +
				"TASK AFTER Download1 COMPLETES AND Download2 COMPLETES AND Download3 COMPLETES IF failed = 1 FAIL END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null); 
	}
}
