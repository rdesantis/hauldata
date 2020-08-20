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
//				"	WRITE XLSX 'email.xlsx' 'Count' FROM EMAIL COUNT WHERE FOLDER 'InBox';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Fields' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY WHERE FOLDER 'InBox';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Attachment Count' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT COUNT WHERE FOLDER 'InBox' SENDER 'desantis';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Attachment Names' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT NAME WHERE FOLDER 'InBox' SENDER 'desantis';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Filtered Attachments' FROM EMAIL SENDER, RECEIVED, SUBJECT, BODY, ATTACHMENT NAME WHERE FOLDER 'InBox' ATTACHMENT NAME 'bage.xlsx';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Detached' FROM EMAIL SENDER, RECEIVED, SUBJECT, ATTACHMENT NAME WHERE FOLDER 'InBox' ATTACHMENT NAME 'garbage' DETACH MARK READ;\n" +
//				"	WRITE XLSX 'email.xlsx' 'Unread' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE UNREAD FOLDER 'InBox' MARK READ;\n" +
//				"	WRITE XLSX 'email.xlsx' 'Received After' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' RECEIVED AFTER '8/13/2020 3:00 PM';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Received Before' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' RECEIVED BEFORE '8/13/2020 3:03 PM';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Received Multi' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' RECEIVED AFTER '8/13/2020 3:00 PM' BEFORE '8/13/2020 3:03 PM';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Sender Multi' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SENDER 'desantis.pmp' OR 'google';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Sender Comma' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SENDER 'ronald.desantis.pmp@gmail.com, rdesantis@cmtgroup.com';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Attachment Filter' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT, ATTACHMENT NAME WHERE FOLDER 'InBox' ATTACHMENT NAME 'bag' AND '.xlsx';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Body Filter' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' BODY 'has';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Body Multi' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' BODY 'has' AND 'attachment';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Subject Filter' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SUBJECT 'testing';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Subject Multi' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SUBJECT 'also' AND 'testing';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Multi Filter' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT, ATTACHMENT NAME WHERE FOLDER 'InBox' SENDER 'desantis' SUBJECT 'testing' AND 'also' ATTACHMENT NAME 'bag' AND '.xlsx';\n" +
//				"	WRITE XLSX 'email.xlsx' 'Another World' FROM EMAIL SELECT SENDER, RECEIVED, SUBJECT WHERE FOLDER 'Another World';\n" +
//				"	WRITE XLSX 'deleted.xlsx' 'Deleted' FROM EMAIL SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SUBJECT 'delete me' DELETE;\n" +
				"	WRITE XLSX 'moved.xlsx' 'Moved' FROM EMAIL SENDER, RECEIVED, SUBJECT WHERE FOLDER 'InBox' SUBJECT 'move me' MOVE TO 'Somewhere Else';\n" +
				"END PROCESS\n" +
				"";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, DbProcessTestTables.assureExist);
	}
}
