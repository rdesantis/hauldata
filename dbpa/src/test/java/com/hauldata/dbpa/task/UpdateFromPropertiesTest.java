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

package com.hauldata.dbpa.task;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class UpdateFromPropertiesTest extends TaskTest {

	public UpdateFromPropertiesTest(String name) {
		super(name);
	}

	public void testUpdateFromProperties() throws Exception {

		String processId = "ErrorMessageTest";
		String script =
				"VARIABLES first VARCHAR, second VARCHAR, third VARCHAR, fourth INTEGER, fifth BIT, sixth DATETIME END VARIABLES \n" +
				"TASK UpdateThem UPDATE first, second, third, fourth, fifth, sixth FROM PROPERTIES 'sample.properties' 'one', 'two', 'three', 'four', 'five', 'six' END TASK \n" +
				"TASK ShowThem AFTER LOG first, second, third, FORMAT(fourth, 'd'), IIF(fifth = 1, '1', '0'), FORMAT(sixth, 'yyyy-MM-dd HH:mm:ss') END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = null;
		Analyzer.Record record = null;

		recordIterator = analyzer.recordIterator(processId, "SHOWTHEM");
		record = recordIterator.next();
		assertEquals("The initial thing", record.message);
		record = recordIterator.next();
		assertEquals("The middle thing", record.message);
		record = recordIterator.next();
		assertEquals("The final thing", record.message);
		record = recordIterator.next();
		assertEquals("122345", record.message);
		record = recordIterator.next();
		assertEquals("1", record.message);
		record = recordIterator.next();
		assertEquals("2020-02-17 15:51:00", record.message);
		assertFalse(recordIterator.hasNext());
	}
}
