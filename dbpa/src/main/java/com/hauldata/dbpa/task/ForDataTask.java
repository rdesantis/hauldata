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

import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;

public class ForDataTask extends UpdateVariablesTask implements TaskSetParent {

	private List<VariableBase> variables;
	private Source source;
	private NestedTaskSet taskSet;

	public ForDataTask(
			Prologue prologue,
			List<VariableBase> variables,
			Source source) {

		super(prologue);

		this.variables = variables;
		this.source = source;
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

			source.executeQuery(context);
			
			while (updateVariables(source, variables)) {
				taskSet.runForRerun(nestedContext);
			}

			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		finally {
			source.close(context);

			if (nestedContext != null) {
				nestedContext.close();
			}
		}
	}
}
