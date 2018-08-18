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

public class RenameTest extends TaskTest {

	public RenameTest(String name) {
		super(name);
	}

	public void testGet() throws Exception {

		String processId = "RenameTest";
		String script =
				"TASK WRITE TXT 'ignore_me.txt' FROM VALUES ('renamed') END TASK \n" +
				"TASK succeeds1 AFTER RENAME 'ignore_me.txt' TO 'ignore_me_' + FORMAT(GETDATE(), 'yyyy-MM-dd_HH-mm-ss') + '.txt' END TASK\n" +
				"TASK succeeds2 AFTER RENAME IF EXISTS 'ignore_me.txt' TO 'this_should_not_fail.txt' END TASK\n" +
				"TASK fails1 AFTER RENAME 'ignore_me.txt' TO 'this_should_fail.txt' END TASK\n" +
				"TASK AFTER fails1 SUCCEEDS FAIL 'fails1 should have failed' END TASK\n" +
				"TASK AFTER fails1 FAILS WRITE TXT 'ignore_me_too.txt' FROM VALUES ('renamed too') END TASK \n" +
				"TASK succeeds3 AFTER RENAME IF EXISTS 'ignore_me_too.txt' TO 'ignore_me_too_' + FORMAT(GETDATE(), 'yyyy-MM-dd_HH-mm-ss') + '.txt' END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
