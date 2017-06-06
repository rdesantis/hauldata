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

import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;

/**
 * RunDbp - Run Database Process.
 * Command line utility to invoke DbProcess
 */
public class RunDbp {

	static final String programName = "RunDbp";
	
	public static void main(String[] args) {

		// Get run options.

		RunOptions.WithArgs optionsWithArgs = RunOptions.get(args);

		RunOptions options = optionsWithArgs.options;
		args = optionsWithArgs.args;

		// Load and go.

		hookProgramExit();

		ContextProperties contextProps = null;
		Context context = null;

		int status = 0;
		try {
			String processID = args[0];
			String[] processArgs = Arrays.copyOfRange(args, 1, args.length);

			contextProps = new ContextProperties(programName);

			context = contextProps.createContext(processID);

			DbProcess process = context.loader.load(processID);

			if (options.isCheckOnly() ) {
				process.validate(processArgs);
			}
			else {
				process.run(processArgs, context);
			}
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			status = 1;
		}
		finally {
			try { if (context != null) context.close(); } catch (Exception ex) {}

			exitProgram(status);
		}
	}

	private static Thread processThread;
	private static boolean isProcessInterrupted;
	private static Thread hookThread;

	private static void hookProgramExit() {
		processThread = Thread.currentThread();
		isProcessInterrupted = false;

		hookThread = new Thread() {
			public void run() {
				isProcessInterrupted = true;
				processThread.interrupt();
				try {
					processThread.join();
				}
				catch (InterruptedException ex) {}
			}
		};

		Runtime.getRuntime().addShutdownHook(hookThread);
	}
	
	private static void exitProgram(int status) {
		if (!isProcessInterrupted) {
			Runtime.getRuntime().removeShutdownHook(hookThread);

			System.exit(status);
		}
	}
}

class RunOptions {

	private boolean checkOnly;

	private RunOptions() {
		checkOnly = false;
	}

	public static RunOptions.WithArgs get(String[] args) {

		RunOptions result = new RunOptions();

		int i = 0;
		while ((i < args.length) && args[i].startsWith("-")) {

			String option = args[i++];
			if (option.startsWith("--")) {
				parseLongOption(result, option.substring(2));
			}
			else {
				parseShortOptions(result, option.substring(1));
			}
		}
		return new RunOptions.WithArgs(result, Arrays.copyOfRange(args, i, args.length));
	}

	private static void parseLongOption(RunOptions result, String option) {

		if (option.equals("check")) {
			result.checkOnly = true;
		}
		else {
			System.err.println("Invalid option: " + option);
			System.exit(1);
		}
	}

	private static void parseShortOptions(RunOptions result, String options) {

		while (options.length() > 0) {
			char option = options.charAt(0);
			options = options.substring(1);

			if (option == 'c') {
				result.checkOnly = true;
			}
			else {
				System.err.println("Invalid option: " + option);
				System.exit(1);
			}
		}
	}

	public boolean isCheckOnly() {
		return checkOnly;
	}

	public static class WithArgs {
		public RunOptions options;
		public String[] args;

		public WithArgs(RunOptions options, String[] args) {
			this.options = options;
			this.args = args;
		}
	}
}
