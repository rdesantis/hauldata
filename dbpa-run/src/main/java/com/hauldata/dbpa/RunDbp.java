/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa;

import java.util.Arrays;

import com.hauldata.dbpa.process.Alert;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.run.RunOptions;
import com.hauldata.dbpa.run.Runner;

/**
 * RunDbp - Run Database Process.
 * Command line utility to invoke DbProcess
 */
public class RunDbp {

	public static final String programName = "RunDbp";

	public static void main(String[] args) {

		RunOptions.WithArgs optionsWithArgs = RunOptions.get(args);
		RunOptions options = optionsWithArgs.getOptions();
		args = optionsWithArgs.getArgs();

		ContextProperties contextProps = null;
		String processID = null;
		Runner runner = null;

		int status = 0;
		try {
			contextProps = new ContextProperties(RunDbp.programName);

			String[] processArgs = null;
			if (0 < args.length) {
				processID = args[0];
				processArgs = Arrays.copyOfRange(args, 1, args.length);
			}

			runner = Runner.get(processID, contextProps, processArgs, options);

			runner.run();
		}
		catch (Exception ex) {

			String failMessage = ex.getMessage();
			System.err.println(failMessage);
			status = 1;

			if (options.isAlert() && (contextProps != null)) {
				Alert.send(processID, contextProps, failMessage);
			}
		}
		finally {
			if (runner != null) {
				runner.close(status);
			}
			else {
				System.exit(status);
			}
		}
	}

	static {QuietLog4j.please();}
}
