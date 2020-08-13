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

import com.hauldata.dbpa.DbProcessTestTables;
import com.hauldata.dbpa.log.Logger.Level;

public class EmailSourceTest extends TaskTest {

	public EmailSourceTest(String name) {
		super(name);
	}

	public void testEmailSource() throws Exception {

		String processId = "EmailSourceTest";
		String script =
				"PROCESS\n" +
				"	VARIABLES test EMAIL;\n" +
				"	CONNECT EMAIL USING 'testdbpa.mail';\n" +
				"	WRITE XLSX 'email.xlsx' 'Count' FROM EMAIL COUNT WHERE FOLDER 'InBox';\n" +
				"	WRITE XLSX 'email.xlsx' 'Fields' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY WHERE FOLDER 'InBox';\n" +
				"	WRITE XLSX 'email.xlsx' 'Attachment Count' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT COUNT WHERE FOLDER 'InBox' SENDER 'desantis';\n" +
				"	WRITE XLSX 'email.xlsx' 'Attachment Names' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT NAME WHERE FOLDER 'InBox' SENDER 'desantis';\n" +
				"	WRITE XLSX 'email.xlsx' 'Filtered Attachments' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT NAME WHERE FOLDER 'InBox' ATTACHMENT NAME 'bage.xlsx';\n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
