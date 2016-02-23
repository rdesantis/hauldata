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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;

public class ForParameterizedStatementTask extends UpdateVariablesTask {

	public ForParameterizedStatementTask(
			Prologue prologue,
			List<VariableBase> variables,
			List<ExpressionBase> expressions,
			String statement,
			NestedTaskSet taskSet) {

		super(prologue);

		this.variables = variables;
		this.expressions = expressions;
		this.statement = statement;
		this.taskSet = taskSet;
	}

	@Override
	protected void execute(Context context) {

		List<Object> values = this.expressions.stream().map(e -> e.getEvaluationObject()).collect(Collectors.toCollection(LinkedList::new));

		Context nestedContext = context.cloneContext();
		nestedContext.log = context.log.nestTask(getName());

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection();

			stmt = prepareParameterizedStatement(values, statement, conn);

			rs = stmt.executeQuery();
			
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
	private List<ExpressionBase> expressions;
	private String statement;
	private NestedTaskSet taskSet;
}
