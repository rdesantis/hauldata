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

import java.util.LinkedList;
import java.util.List;

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
}
