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

package com.hauldata.dbpa.task;

import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableBase;

public class RunCommandTask extends Task {

	private Expression<String> name;
	private List<ExpressionBase> arguments;
	private VariableBase resultParam;

	public RunCommandTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments,
			VariableBase resultParam) {

		super(prologue);
		this.name = name;
		this.arguments = arguments;
		this.resultParam = resultParam;
	}

	@Override
	protected void execute(Context context) throws Exception {

		String commandName = name.evaluate();
		if (commandName == null) {
			throw new RuntimeException("Command expression evaluates to null");
		}

		List<String> command = new LinkedList<String>();
		command.add(commandName);

		if (arguments != null) {
			for (ExpressionBase argument : arguments) {
				Object argumentValue = argument.getEvaluationObject();
				command.add((argumentValue != null) ? argumentValue.toString() : "");
			}
		}

		Process process = new ProcessBuilder(command).inheritIO().start();

		try {
			int exitValue = process.waitFor();
			if (resultParam != null) {
				resultParam.setValueObject((Integer)exitValue);
			}
		}
		catch (InterruptedException iex) {
			// This RUN COMMAND task was interrupted.  Terminate the operating system process.
			process.destroy();
		}
	}
}
