/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

public class AppendTaskTest extends TaskTest {

	public AppendTaskTest(String name) {
		super(name);
	}

	public void testAppendNotExist() throws Exception {
		
		String processId = "AppendTest";
		String script =
				"TASK DeleteIt \n" +
				"	DELETE 'does not exist.csv' \n" +
				"END TASK\n" +
				"TASK AppendDoesNotExist AFTER \n" +
				"	APPEND CSV 'does not exist.csv' \n" +
				"	FROM VALUES (1) \n" +
				"END TASK\n" +
				"TASK AppendDoesExist AFTER \n" +
				"	APPEND CSV 'does not exist.csv' \n" +
				"	FROM VALUES (2) \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}