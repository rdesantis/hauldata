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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class LogTaskTest extends LogTaskTestBase {

	public LogTaskTest(String name) {
		super(name);
	}

	public void testLogTask() throws Exception {

		String processId = "LogTest";
		String script = "TASK TEST LOG 'This are a test' END TASK";

		testLogTask(processId, Level.info, true, script, "This are a test");
		testLogTask(processId, Level.message, true, script, "This are a test");
	}

	public void testMultiLogTask() throws Exception {

		String processId = "LogTest";
		String script = "TASK TEST LOG 'This are yes a test', 'So are this', 'and so on' END TASK";

		List<String> messages = new LinkedList<String>();
		messages.add("This are yes a test");
		messages.add("So are this");
		messages.add("and so on");

		testLogTask(processId, Level.info, true, script, messages);
		testLogTask(processId, Level.message, true, script, messages);
	}

	public void testLogNegativeTest() {

		String script = "TASK LOG 'Next week is ' + FORMAT(DATEADD(WEEK, 1, GETDATE()), 'M/d/yyyy') END TASK";

		assertBadSyntax(script, "At line 1: Unrecognized date part name for DATEADD");
	}

	public void testLogQualifers() throws Exception {

		String processId = "QualifierTest";
		String script =
				"VARIABLES \n" +
					"first VARCHAR, second VARCHAR, third VARCHAR \n" +
				"END VARIABLES \n" +
				"TASK Assign SET \n" +
					"first = 'one', \n" +
					"second = 'two', \n" +
					"third = 'three' \n" +
				"END TASK \n" +
				"TASK log1 OF first AFTER LOG 'uno' END TASK \n" +
				"TASK log2 OF second AFTER log1 LOG 'due' END TASK \n" +
				"TASK OF first + '_' + second AFTER log2 LOG 'uno_due' END TASK \n" +
				"TASK Nester OF third AFTER PROCESS 'Inner' WITH third END TASK \n" +
				"";

		String nestedScriptName = "Inner";
		String nestedScript =
				"PARAMETERS parm VARCHAR END PARAMETERS\n" +
				"TASK nest1 OF 'this' LOG 'tre' END TASK \n" +
				"";

		Map<String, String> nestedScripts = new HashMap<String, String>();
		nestedScripts.put(nestedScriptName, nestedScript);

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, nestedScripts, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, Pattern.compile("LOG\\d+.*"));
		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals("LOG1:one", record.taskId);
		assertEquals("uno", record.message);

		record = recordIterator.next();
		assertEquals("LOG2:two", record.taskId);
		assertEquals("due", record.message);

		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, Pattern.compile("\\d+.*"));

		record = recordIterator.next();
		assertEquals("4:one_two", record.taskId);
		assertEquals("uno_due", record.message);

		assertFalse(recordIterator.hasNext());

		String nestedProcessId = processId + "[NESTER:three]." + nestedScriptName;

		recordIterator = analyzer.recordIterator(nestedProcessId, Pattern.compile("NEST\\d+.*"));

		record = recordIterator.next();
		assertEquals("NEST1:this", record.taskId);
		assertEquals("tre", record.message);

		assertFalse(recordIterator.hasNext());
	}
}
