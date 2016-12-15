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

import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class SetTest extends TaskTest {

	public SetTest(String name) {
		super(name);
	}

	public void testSet() throws Exception {

		String processId = "SetTest";
		String script =
				"VARIABLES \n" +
					"i1 INT, \n" +
					"i2 INT, \n" +
					"i3 INT, \n" +
					"s1 VARCHAR, \n" +
					"s2 VARCHAR, \n" +
					"s3 VARCHAR, \n" +
					"s4 VARCHAR, \n" +
					"s5 VARCHAR, \n" +
					"s6 VARCHAR, \n" +
					"s7 VARCHAR, \n" +
					"s8 VARCHAR, \n" +
					"s9 VARCHAR, \n" +
					"s10 VARCHAR \n" +
				"END VARIABLES \n" +
				"TASK SET \n" +
					"i1 = CHARINDEX('678', '12345678901234567890'), \n" +
					"i2 = CHARINDEX('678', '12345678901234567890', i1 + 1), \n" +
					"s1 = LEFT('1234567890', 5), \n" +
					"s2 = LTRIM('   123'), \n" +
					"i3 = LEN('1234567890'), \n" +
					"s3 = LOWER('ABC'), \n" +
					"s4 = REPLACE('1234', '23', '32'), \n" +
					"s5 = REPLICATE('123', 3), \n" +
					"s6 = RIGHT('1234567890', 5), \n" +
					"s7 = RTRIM('123   '), \n" +
					"s8 = SPACE(3), \n" +
					"s9 = SUBSTRING('1234567890', 4, 3), \n" +
					"s10 = UPPER('xyz') \n" +
				"END TASK \n" +
				"TASK Show AFTER DO \n" +
					"TASK LOG FORMAT(i1, 'd') END TASK \n" +
					"TASK AFTER LOG FORMAT(i2, 'd') END TASK \n" +
					"TASK AFTER LOG s1 END TASK \n" +
					"TASK AFTER LOG s2 END TASK \n" +
					"TASK AFTER LOG FORMAT(i3, 'd') END TASK \n" +
					"TASK AFTER LOG s3 END TASK \n" +
					"TASK AFTER LOG s4 END TASK \n" +
					"TASK AFTER LOG s5 END TASK \n" +
					"TASK AFTER LOG s6 END TASK \n" +
					"TASK AFTER LOG s7 END TASK \n" +
					"TASK AFTER LOG s8 END TASK \n" +
					"TASK AFTER LOG s9 END TASK \n" +
					"TASK AFTER LOG s10 END TASK \n" +
				"END TASK \n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);

		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(processId, Pattern.compile("SHOW\\..*"));
		Analyzer.Record record = null;

		record = recordIterator.next();
		assertEquals("6", record.message);
		record = recordIterator.next();
		assertEquals("16", record.message);
		record = recordIterator.next();
		assertEquals("12345", record.message);
		record = recordIterator.next();
		assertEquals("123", record.message);
		record = recordIterator.next();
		assertEquals("10", record.message);
		record = recordIterator.next();
		assertEquals("abc", record.message);
		record = recordIterator.next();
		assertEquals("1324", record.message);
		record = recordIterator.next();
		assertEquals("123123123", record.message);
		record = recordIterator.next();
		assertEquals("67890", record.message);
		record = recordIterator.next();
		assertEquals("123", record.message);
		record = recordIterator.next();
		assertEquals("   ", record.message);
		record = recordIterator.next();
		assertEquals("456", record.message);
		record = recordIterator.next();
		assertEquals("XYZ", record.message);

		assertFalse(recordIterator.hasNext());
	}
}
