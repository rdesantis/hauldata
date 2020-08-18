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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.variable.VariableBase;

public class SyncProcessTask extends ProcessTask {

	public SyncProcessTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments,
			VariableBase returnVariable,
			Map<String, DbProcess> siblingProcesses) {

		super(prologue, name, arguments, returnVariable, siblingProcesses);
	}

	@Override
	protected void execute(Context context) {

		if (context.loader == null) {
			throw new RuntimeException("Process loader is not available");
		}

		String processName = name.evaluate();

		List<Object> args = new LinkedList<Object>();
		for (ExpressionBase argument : arguments) {
			Object argumentValue = argument.getEvaluationObject();
			args.add(argumentValue);
		}

		Context childContext = context.makeChildContext(getName(), processName);
		try {
			DbProcess process = siblingProcesses.get(processName.toUpperCase());
			if (process == null) {
				process = context.loader.load(processName);
			}
			Object returnValue = process.run(args, childContext);
			if (returnVariable != null) {
				returnVariable.setValueChecked(returnValue);
			}
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to load or run process " + processName + " - " + message, ex);
		}
		finally {
			childContext.close();
		}
	}
}
