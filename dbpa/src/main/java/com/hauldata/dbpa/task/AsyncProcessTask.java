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

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.expression.StringConstant;
import com.hauldata.dbpa.process.Context;

public class AsyncProcessTask extends ProcessTask {

	public AsyncProcessTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments) {

		super(prologue, name, arguments);
	}

	@Override
	protected void execute(Context context) {

		Expression<String> evaluatedName = new StringConstant(name.evaluate());

		List<ExpressionBase> evaluatedArguments = new LinkedList<ExpressionBase>();
		for (ExpressionBase argument : arguments) {
			Object argumentValue = argument.getEvaluationObject();
			evaluatedArguments.add(new StringConstant(argumentValue != null ? argumentValue.toString() : null));
		}
		
		String asyncTaskName = getName() + ".async";
		SyncProcessTask task = new SyncProcessTask(new Prologue(asyncTaskName, null, null, null), evaluatedName, evaluatedArguments);
		context.rootExecutor.submit(task, context);
	}
}
