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

import com.hauldata.dbpa.DbProcessTestProperties;
import com.hauldata.dbpa.DbProcessTestPropertiesImpl;
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

	protected Analyzer runScript(
			String processId,
			Level logLevel,
			boolean logToConsole,
			String script,
			String[] args,
			Map<String, String> nestedScripts) throws Exception {

		DbProcessTestProperties testProps = new DbProcessTestPropertiesImpl();

		Context context = new Context(
				testProps.getConnectionProperties(),
				testProps.getMailProperties(),
				testProps.getFtpProperties(),
				testProps.getDataPath(),
				new TestLoader(nestedScripts));

		RootLogger logger = new RootLogger(processId, logLevel);
		Analyzer analyzer = new Analyzer();
		logger.add(analyzer);

		if (logToConsole) {
			logger.add(new ConsoleAppender());
		}

		context.logger = logger;

		if (args == null) {
			args = new String[0];
		}

		try {
			DbProcess process = DbProcess.parse(new StringReader(script));
			process.run(args, context);
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			throw ex;
		}
		finally {
			logger.close();
		}

		return analyzer;
	}
}
