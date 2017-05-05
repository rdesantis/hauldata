/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.expression;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.task.TaskTest;

public class ErrorMessageTest extends TaskTest {

	public ErrorMessageTest(String name) {
		super(name);
	}

	public void testErrorMessage() throws Exception {

		String processId = "ErrorMessageTest";
		String script =
				"TASK NoError LOG ISNULL(ERROR_MESSAGE(), 'No error') END TASK \n" +
				"TASK Good1 GO END TASK \n" +
				"TASK Good2 GO END TASK \n" +
				"TASK Bad1 FAIL 'Bad1 failed' END TASK \n" +
				"TASK CatchAll AFTER Good1 FAILS OR Good2 FAILS OR Bad1 FAILS DO \n" +
				"	TASK Skip1 GO END TASK \n" +
				"	TASK Skip2 GO END TASK \n" +
				"	TASK Inner1 AFTER Skip1 AND Skip2 LOG 'Error message = <' + ERROR_MESSAGE() + '>' END TASK \n" +
				"END TASK \n" +
				"TASK Direct1 AFTER Bad1 FAILS LOG 'Error message = <' + ERROR_MESSAGE() + '>' END TASK \n" +
				"TASK Indirect1 AFTER Good1 AND Direct1 LOG 'Error message = <' + ERROR_MESSAGE() + '>' END TASK \n" +
				"";

		String message = "Error message = <Bad1 failed>";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator(processId, "DIRECT1");
		record = recordIterator.next();
		assertEquals(message, record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "INDIRECT1");
		record = recordIterator.next();
		assertEquals(message, record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "CATCHALL.INNER1");
		record = recordIterator.next();
		assertEquals(message, record.message);
		assertFalse(recordIterator.hasNext());

		recordIterator = analyzer.recordIterator(processId, "NOERROR");
		record = recordIterator.next();
		assertEquals("No error", record.message);
		assertFalse(recordIterator.hasNext());
	}
}
