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
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.variable.VariableBase;

public abstract class ProcessTask extends Task {

	protected Expression<String> name;
	protected List<ExpressionBase> arguments;
	protected VariableBase returnVariable;
	protected Map<String, DbProcess> siblingProcesses;

	public ProcessTask(
			Prologue prologue,
			Expression<String> name,
			List<ExpressionBase> arguments,
			VariableBase returnVariable,
			Map<String, DbProcess> siblingProcesses) {

		super(prologue);
		this.name = name;
		this.arguments = arguments;
		this.returnVariable = returnVariable;
		this.siblingProcesses = siblingProcesses;
	}
}
