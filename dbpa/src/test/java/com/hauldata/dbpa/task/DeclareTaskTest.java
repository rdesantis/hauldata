package com.hauldata.dbpa.task;

import com.hauldata.dbpa.log.Logger.Level;

public class DeclareTaskTest extends TaskTest {

	public DeclareTaskTest(String name) {
		super(name);
	}

	public void testDoLoopTask() throws Exception {

		String processId = "DeclareTest";
		String script =
				"PROCESS\n" +
				"GO;\n" +
				"DECLARE i0 INTEGER, i INTEGER = 123, v VARCHAR, b BIT = 0, b0 BIT, db DATABASE, i456 INTEGER = i + 333;\n" +
				"IF i456 <> 456 FAIL;\n" +
				"SET v = 'test';\n" +
				"fred: DECLARE message VARCHAR = 'This is a ' + v;\n" +
				"IF message <> 'This is a test' FAIL;\n" +
				"END PROCESS\n";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
