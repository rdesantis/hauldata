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

import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;

public class SyncProcessTask extends ProcessTask {

	public SyncProcessTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments) {

		super(prologue, name, arguments);
	}

	@Override
	protected void execute(Context context) {

		if (context.loader == null) {
			throw new RuntimeException("Process loader is not available");
		}

		String processName = name.evaluate();
		String[] args = new String[arguments.size()];
		
		int i = 0;
		for (ExpressionBase argument : arguments) {
			Object argumentValue = argument.getEvaluationObject();
			args[i++] = (argumentValue != null) ? argumentValue.toString() : null;
		}
		
		Context nestedContext = context.nestContext();
		nestedContext.log = context.log.nestProcess(processName);
		try {
			DbProcess process = context.loader.load(processName);
			process.run(args, nestedContext);
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to load or run process " + processName + " - " + message, ex);
		}
		finally {
			nestedContext.closeNested();
		}
	}
}
