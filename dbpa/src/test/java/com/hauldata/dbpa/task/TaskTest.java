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

import java.io.StringReader;
import java.util.Map;

import com.hauldata.dbpa.DbProcessTestPropertiesBase;
import com.hauldata.dbpa.QuietLog4j;
import com.hauldata.dbpa.loader.TestLoader;
import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.ConsoleAppender;
import com.hauldata.dbpa.log.RootLogger;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;

import junit.framework.TestCase;

public abstract class TaskTest extends TestCase {

	protected TaskTest(String name) {
		super(name);
	}

	public static interface ContextAction {
		public void action(Context context);
	}

	protected Analyzer runScript(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script,
			String[] args,
			Map<String, String> nestedScripts,
			ContextAction setup,
			boolean exceptionOnTaskFailed) throws Exception {

		final String testPropsClass = "com.hauldata.dbpa.DbProcessTestProperties";
		DbProcessTestPropertiesBase testProps = null;
		try {
			testProps = (DbProcessTestPropertiesBase) Class.forName(testPropsClass).newInstance();
		}
		catch (Exception ex) {
			System.err.println("Error attempting to load class " + testPropsClass);
			System.err.println(ex.getLocalizedMessage());
			System.err.println("You must provide an implementation of this class in order to run task tests");
			throw ex;
		}

		Context context = new Context(
				testProps.getConnectionProperties(),
				testProps.getMailProperties(),
				testProps.getFtpProperties(),
				testProps.getPathProperties(),
				new TestLoader(nestedScripts));

		RootLogger logger = new RootLogger(processId);
		Analyzer analyzer = new Analyzer(logLevel);
		logger.add(analyzer);

		if (logToConsole) {
			logger.add(new ConsoleAppender(logLevel));
		}

		context.logger = logger;

		if (setup != null) {
			setup.action(context);
		}

		if (args == null) {
			args = new String[0];
		}

		DbProcess process = null;
		try {
			process = DbProcess.parse(new StringReader(script));
			process.run(args, context);
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			if ((process == null) || exceptionOnTaskFailed) {
				throw ex;
			}
		}
		finally {
			context.close();
		}

		return analyzer;
	}

	protected Analyzer runScript(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script,
			String[] args,
			Map<String, String> nestedScripts,
			ContextAction setup) throws Exception {
		return runScript(processId, logLevel, logToConsole, script, args, nestedScripts, setup, true);
	}

	protected void assertBadSyntax(String script, String expectedMessage) {

		DbProcess process = null;
		String message = null;

		try {
			process = DbProcess.parse(new StringReader(script));
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}

		assertNull(process);
		if (expectedMessage != null) {
			assertEquals(expectedMessage, message);
		}
	}

	protected void assertGoodSyntax(String script) {

		DbProcess process = null;

		try {
			process = DbProcess.parse(new StringReader(script));
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
		}

		assertNotNull(process);
	}

	public void assertScriptFails(
			String processId,
			String script,
			String taskName,
			String failMessage) throws Exception {

		Level logLevel = Level.error;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null, false);

		Analyzer.RecordIterator iterator = analyzer.recordIterator(processId, taskName);
		Analyzer.Record record;

		record = iterator.next();
		assertEquals(failMessage, record.message);

		record = iterator.next();
		assertEquals(Task.failMessage, record.message);

		assertFalse(iterator.hasNext());
	}

	static {QuietLog4j.please();}
}
