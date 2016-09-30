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

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableBase;

public class UpdateFromStatementTask extends UpdateVariablesTask {

	private List<VariableBase> variables;
	private DatabaseConnection connection;
	private Expression<String> statement;

	public UpdateFromStatementTask(
			Prologue prologue,
			List<VariableBase> variables,
			DatabaseConnection connection,
			Expression<String> statement) {

		super(prologue);
		this.variables = variables;
		this.connection = connection;
		this.statement = statement;
	}

	@Override
	protected void execute(Context context) {

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(connection);

			stmt = createOneRowStatement(conn);

			rs = stmt.executeQuery(statement.evaluate());

			updateVariablesOnce(rs, variables);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		finally { try {

			if (rs != null) rs.close();
			if (stmt != null) stmt.close();
		}
		catch (SQLException ex) {
			throwDatabaseCloseFailed(ex);
		}
		finally {
			if (conn != null) context.releaseConnection(connection);
		} }
	}
}
