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

package com.hauldata.dbpa.run;

import java.util.Arrays;

import com.hauldata.dbpa.RunDbp;

public class RunOptions {

	private boolean versionOnly = false;
	private boolean checkOnly = false;
	private boolean alert = false;
	private String scheduleName = null;

	private RunOptions() {}

	public static RunOptions.WithArgs get(String[] args) {

		RunOptions result = new RunOptions();

		int i = 0;
		try {
			while ((i < args.length) && args[i].startsWith("-")) {

				String option = args[i++];
				if (option.startsWith("--")) {
					if (option.equals("--")) {
						break;
					}
					parseLongOption(result, option.substring(2));
				}
				else {
					parseShortOptions(result, option.substring(1));
				}
			}
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}

		if (result.versionOnly) {
			String version = RunDbp.class.getPackage().getImplementationVersion();
			System.out.println(version);
			System.exit(0);
		}

		return new RunOptions.WithArgs(result, Arrays.copyOfRange(args, i, args.length));
	}

	private static void parseLongOption(RunOptions result, String option) {

		if (option.equals("version")) {
			result.versionOnly = true;
		}
		else if (option.equals("check")) {
			result.checkOnly = true;
		}
		else if (option.equals("alert")) {
			result.alert = true;
		}
		else if (option.equals("schedule")) {
			throw new RuntimeException("Schedule option must be followed by :file_name");
		}
		else if (option.startsWith("schedule:")) {
			String[] tokens = option.split(":");
			if (tokens.length == 1) {
				throw new RuntimeException("Missing file name in schedule option");
			}
			else if (tokens.length != 2) {
				throw new RuntimeException("Invalid format for schedule option: " + option);
			}
			result.scheduleName = tokens[1];
		}
		else {
			throw new RuntimeException("Invalid option: " + option);
		}
	}

	private static void parseShortOptions(RunOptions result, String options) {

		while (options.length() > 0) {
			char option = options.charAt(0);
			options = options.substring(1);

			if (option == 'v') {
				result.versionOnly = true;
			}
			else if (option == 'c') {
				result.checkOnly = true;
			}
			else if (option == 'a') {
				result.alert = true;
			}
			else {
				throw new RuntimeException("Invalid option: " + option);
			}
		}
	}

	public boolean isCheckOnly() {
		return checkOnly;
	}

	public boolean isAlert() {
		return alert;
	}

	public boolean isScheduled() {
		return (scheduleName != null);
	}

	public String getScheduleName() {
		return scheduleName;
	}

	public static class WithArgs {
		private RunOptions options;
		private String[] args;

		public WithArgs(RunOptions options, String[] args) {
			this.options = options;
			this.args = args;
		}

		public RunOptions getOptions() {
			return options;
		}

		public String[] getArgs() {
			return args;
		}
	}
}