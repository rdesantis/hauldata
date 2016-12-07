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

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class ForFilesTaskTest extends TaskTest {

	public ForFilesTaskTest(String name) {
		super(name);
	}

	public void testForFiles() throws Exception {

		String processId = "ForFilesTest";
		String script = 
				"VARIABLES file_name VARCHAR END VARIABLES \n" +
				"TASK Loop FOR file_name FROM FILES '*.csv' \n" +
					"TASK Echo LOG file_name END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null); 

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");
		Analyzer.Record record = null;

		while (recordIterator.hasNext()) {
			record = recordIterator.next();
			assertTrue(record.message.endsWith(".csv"));
		}
	}
}
