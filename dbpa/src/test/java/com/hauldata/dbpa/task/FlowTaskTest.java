/*
 * Copyright (c) 2020 Ronald DeSantis
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

public class FlowTaskTest extends TaskTest {

	public FlowTaskTest(String name) {
		super(name);
	}

	public void testFlowFromValuesToSql() throws Exception {
		
		String processId = "FlowValuesToSqlTest";
		String script =
				"PROCESS\n" +
				"DECLARE i INT, v VARCHAR;\n" +
				"RUN SQL TRUNCATE TABLE test.importtarget END SQL;\n" +
				"FLOW FROM VALUES (246, 'sticks'), (789, 'fine') INTO SQL INSERT INTO test.importtarget VALUES (?,?) END SQL;\n" +
				"FOR i, v FROM TABLE 'test.importtarget'\n" +
				"	OF FORMAT(i, 'd') + ' ' + v:\n" +
				"	DECLARE expected VARCHAR =\n" +
				"		CASE i\n" +
				"		WHEN 246 THEN 'sticks'\n" +
				"		WHEN 789 THEN 'fine'\n" +
				"		ELSE NULL\n" +
				"		END;\n" +
				"	IF expected IS NULL OR v <> expected FAIL;\n" +
				"END FOR \n" +
				"END PROCESS\n";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}

	public void testFlowFromValuesToTable() throws Exception {

		String processId = "FlowValuesToTableTest";
		String script =
				"PROCESS\n" +
				"DECLARE i INT, v VARCHAR;\n" +
				"RUN SQL TRUNCATE TABLE test.importtarget END SQL;\n" +
				"FLOW FROM VALUES (123, 'tree'), (457, 'heaven') INTO TABLE 'test.importtarget';\n" +
				"FOR i, v FROM TABLE 'test.importtarget'\n" +
				"	OF FORMAT(i, 'd') + ' ' + v:\n" +
				"	DECLARE expected VARCHAR =\n" +
				"		CASE i\n" +
				"		WHEN 123 THEN 'tree'\n" +
				"		WHEN 457 THEN 'heaven'\n" +
				"		ELSE NULL\n" +
				"		END;\n" +
				"	IF expected IS NULL OR v <> expected FAIL;\n" +
				"END FOR \n" +
				"END PROCESS\n";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}

	public void testFlowFromSqlToTable() throws Exception {

		String processId = "FlowSqlToTableTest";
		String script =
				"PROCESS\n" +
				"RUN SQL TRUNCATE TABLE test.importtarget END SQL;\n" +
				"FLOW FROM SQL SELECT size AS number, name AS word FROM test.things END SQL INTO TABLE 'test.importtarget';\n" +
				"END PROCESS\n";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
