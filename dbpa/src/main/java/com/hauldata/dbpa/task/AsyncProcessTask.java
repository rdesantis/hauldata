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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.expression.StringConstant;
import com.hauldata.dbpa.process.Context;

public class AsyncProcessTask extends ProcessTask {

	private Set<Task> asyncSuccessors;

	public AsyncProcessTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments) {

		super(prologue, name, arguments);
		asyncSuccessors = new HashSet<Task>();
	}

	public void addAsyncSuccessor(Task task) {
		asyncSuccessors.add(task);
	}

	@Override
	protected void execute(Context context) {

		Expression<String> evaluatedName = new StringConstant(name.evaluate());

		List<ExpressionBase> evaluatedArguments = new LinkedList<ExpressionBase>();
		for (ExpressionBase argument : arguments) {
			Object argumentValue = argument.getEvaluationObject();
			evaluatedArguments.add(new StringConstant(argumentValue != null ? argumentValue.toString() : null));
		}
		
		String asyncTaskName = getName() + "-async";
		Map<Task, Task.Result> noPredecessors = new HashMap<Task, Task.Result>();
		SyncProcessTask processTask = new SyncProcessTask(new Prologue(asyncTaskName, null, noPredecessors, null, null, null), evaluatedName, evaluatedArguments);

		for (Task waitTask : asyncSuccessors) {
			waitTask.putRemainingPredecessor(processTask, Result.completed);
			processTask.addSuccessor(waitTask);
		}

		context.rootExecutor.submit(processTask, context);
	}
}
