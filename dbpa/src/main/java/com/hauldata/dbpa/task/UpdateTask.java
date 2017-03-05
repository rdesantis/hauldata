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
import com.hauldata.dbpa.variable.VariableBase;

public class UpdateTask extends UpdateVariablesTask {

	private List<VariableBase> variables;
	private Source source;

	public UpdateTask(
			Prologue prologue,
			List<VariableBase> variables,
			Source source) {

		super(prologue);
		this.variables = variables;
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		try {
			source.executeQuery(context);

			updateVariablesOnce(source, variables);
			
			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Update terminated due to interruption");
		}
		finally {
			source.close(context);
		}
	}
}
