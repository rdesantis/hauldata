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

		ContextProperties contextProps = null;
		Context context = null;

		int status = 0;
		try {
			String processID = args[0];
			String[] processArgs = Arrays.copyOfRange(args, 1, args.length);

			contextProps = new ContextProperties(programName);

			context = contextProps.createContext(processID);

			DbProcess process = context.loader.load(processID);
			
			process.run(processArgs, context);
		}
		catch (Exception ex) {
			System.err.println(ex.getMessage());
			status = 1;
		}
		finally {
			try { if (context != null) context.close(); } catch (Exception ex) {}
			
			System.exit(status);
		}
	}
}
