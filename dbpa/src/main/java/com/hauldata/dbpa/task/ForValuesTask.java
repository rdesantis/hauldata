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

import java.util.ArrayList;
import java.util.List;

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;

public class ForValuesTask extends Task implements TaskSetParent {

	private ArrayList<VariableBase> variables;
	private List<ExpressionBase[]> values;
	private NestedTaskSet taskSet;

	public ForValuesTask(
			Task.Prologue prologue,
			ArrayList<VariableBase> variables,
			List<ExpressionBase[]> values) {

		super(prologue);
		this.variables = variables;
		this.values = values;
	}

	@Override
	public Task setTaskSet(NestedTaskSet taskSet) {
		this.taskSet = taskSet;
		return this;
	}

	@Override
	public NestedTaskSet getTaskSet() {
		return taskSet;
	}

	@Override
	protected void execute(Context context) throws Exception {

		Context nestedContext = null;
		try {
			nestedContext = context.makeNestedContext(getName());

			for (ExpressionBase[] expressionList : values) {

				int index = 0;
				for (VariableBase variable : variables) {
					variable.setValueObject(expressionList[index++].getEvaluationObject());
				}

				taskSet.runForRerun(nestedContext);
			}
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Loop terminated due to interruption");
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to run nested tasks - " + message, ex);
		}
		finally {
			if (nestedContext != null) {
				nestedContext.close();
			}
		}
	}

}
