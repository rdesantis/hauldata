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

public class PutTaskTest extends TaskTest {

	public PutTaskTest(String name) {
		super(name);
	}

	public void testPut() throws Exception {

		String processId = "PutTest";
		String script = 
				"TASK Upload PUT 'test file.csv' '/root/staff/test' END TASK \n" +
				"TASK Upload2 PUT 'another test file.xlsx', 'written.csv' TO '/root/staff/test' END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null); 
	}
}
