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

public class ForValuesTaskTest extends TaskTest {

	public ForValuesTaskTest(String name) {
		super(name);
	}

	public void testForValues() throws Exception {

		String processId = "ForValuesTest";
		String script = 
				"VARIABLES equals_zero TINYINT, card_type VARCHAR END VARIABLES \n" +
				"TASK Loop FOR equals_zero, card_type FROM VALUES (5 - 4, 'Am' + 'ex'), (0, 'Other') \n" +
					"TASK Echo LOG '(' + FORMAT(equals_zero, 'd') + ', ' + '''' + card_type + ''')' END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null); 

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, "LOOP.ECHO");
		Analyzer.Record record = null;

		record = recordIterator.next();
		assertEquals("(1, 'Amex')", record.message);

		record = recordIterator.next();
		assertEquals("(0, 'Other')", record.message);

		assertFalse(recordIterator.hasNext());
	}
}
