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

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableBase;

public class SetVariablesTask extends Task {

	public static class Assignment {
		public VariableBase variable;
		public ExpressionBase expression;
		
		public Assignment(VariableBase variable, ExpressionBase expression) {
			this.variable = variable;
			this.expression = expression;
		}
	}

	public SetVariablesTask(
			Prologue prologue,
			List<Assignment> assignments) {
		
		super(prologue);
		this.assignments = assignments;
	}

	@Override
	protected void execute(Context context) {

		for (Assignment assignment : assignments) {
			assignment.variable.setValueObject(assignment.expression.getEvaluationObject());
		}
	}

	private List<Assignment> assignments;
}
