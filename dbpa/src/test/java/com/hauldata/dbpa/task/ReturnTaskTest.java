/*
 * Copyright (c) 2020, Ronald DeSantis
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

import java.util.HashMap;
import java.util.Map;

import com.hauldata.dbpa.log.Logger.Level;

public class ReturnTaskTest extends TaskTest {

	public ReturnTaskTest(String name) {
		super(name);
	}

	public void testLocalReturn() throws Exception {
		String processId = "LocalReturnTest";
		String script =
				"PROCESS\n" +
				"DECLARE result VARCHAR = 'original'; \n" +
				"RUN PROCESS 'local' RETURNING result;\n" +
				"IF result <> 'returned' FAIL;\n" +
				"END PROCESS\n" +
				"PROCESS local\n" +
				"RETURN 'returned';\n" +
				"END PROCESS\n";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}

	public void testExternalReturn() throws Exception {
		String processId = "ExernalReturnTest";
		String script =
				"PROCESS\n" +
				"DECLARE sum INTEGER = 0; \n" +
				"RUN PROCESS 'add' 1, 2 RETURNING sum;\n" +
				"IF sum <> 3 FAIL;\n" +
				"DECLARE glob VARCHAR = ''; \n" +
				"RUN PROCESS 'concat' 'foo', 'bar' RETURNING glob;\n" +
				"IF glob <> 'foobar' FAIL;\n" +
				"END PROCESS\n";

		String addScript =
				"PROCESS (i INTEGER, j INTEGER)\n" +
				"RETURN i + j;\n" +
				"END PROCESS\n";

		String concatScript =
				"PROCESS (i VARCHAR, j VARCHAR)\n" +
				"RETURN i + j;\n" +
				"END PROCESS\n";

		Map<String, String> externalScripts = new HashMap<String, String>();
		externalScripts.put("add", addScript);
		externalScripts.put("concat", concatScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, externalScripts, null);
	}

	public void testAsyncReturn() throws Exception {
		String processId = "AsyncReturnTest";
		String script =
				"PROCESS\n" +
				"DECLARE sum INTEGER = 0; \n" +
				"RUN PROCESS ASYNC 'SlowAdd' 4, 5 RETURNING sum;\n" +
				"RUN PROCESS ASYNC 'Add' 1, 2 RETURNING sum;\n" +
				"wait_here: WAITFOR ASYNC;\n" +
				"LOG 'sum = ' + FORMAT(sum, 'd');\n" +
				"IF sum <> 9 FAIL;\n" +
				"END PROCESS\n";

		String slowAddScript =
				"PROCESS (i INTEGER, j INTEGER)\n" +
				"WAITFOR DELAY '0:00:03';\n" +
				"RETURN i + j;\n" +
				"END PROCESS\n";

		String addScript =
				"PROCESS (i INTEGER, j INTEGER)\n" +
				"RETURN i + j;\n" +
				"END PROCESS\n";

		Map<String, String> externalScripts = new HashMap<String, String>();
		externalScripts.put("SlowAdd", slowAddScript);
		externalScripts.put("Add", addScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, externalScripts, null);
	}
}
