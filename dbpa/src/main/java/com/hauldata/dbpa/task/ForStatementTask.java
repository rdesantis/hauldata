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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;

public class ForStatementTask extends UpdateVariablesTask {

	public ForStatementTask(
			Prologue prologue,
			List<VariableBase> variables,
			Expression<String> statement,
			NestedTaskSet taskSet) {
	
		super(prologue);
	
		this.variables = variables;
		this.statement = statement;
		this.taskSet = taskSet;
	}
	
	@Override
	protected void execute(Context context) {
	
		Context nestedContext = context.cloneContext();
		nestedContext.logger = context.logger.nestTask(getName());

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
	
		try {
			conn = context.getConnection();

			stmt = createStatement(conn);

			rs = stmt.executeQuery(statement.evaluate());
			
			while (updateVariables(rs, variables)) {
				taskSet.runForRerun(nestedContext);
			}
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
		finally { try {
	
			if (rs != null) rs.close();
			if (stmt != null) stmt.close();
		}
		catch (SQLException ex) {
			throwDatabaseCloseFailed(ex);
		}
		finally {
			if (conn != null) context.releaseConnection();

			nestedContext.closeCloned();
		} }
	}
	
	private List<VariableBase> variables;
	private Expression<String> statement;
	private NestedTaskSet taskSet;
}
