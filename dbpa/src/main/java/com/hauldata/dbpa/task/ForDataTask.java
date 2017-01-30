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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;

public class ForDataTask extends UpdateVariablesTask implements TaskSetParent {

	private List<VariableBase> variables;
	private DataSource dataSource;
	private NestedTaskSet taskSet;

	public ForDataTask(
			Prologue prologue,
			List<VariableBase> variables,
			DataSource dataSource) {

		super(prologue);

		this.variables = variables;
		this.dataSource = dataSource;
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
	protected void execute(Context context) {

		Context nestedContext = context.makeNestedContext(getName());

		try {
			ResultSet rs = dataSource.executeQuery(context);
			
			while (updateVariables(rs, variables)) {
				taskSet.runForRerun(nestedContext);
			}

			dataSource.done(context);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Loop terminated due to interruption");
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to run nested tasks - " + message, ex);
		}
		finally {
			dataSource.close(context);

			nestedContext.close();
		}
	}
}
