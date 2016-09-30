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
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;

public class RunParameterizedStatementTask extends DatabaseTask {

	private DatabaseConnection connection;
	private List<ExpressionBase> expressions;
	private String statement;

	public RunParameterizedStatementTask(
			Prologue prologue,
			DatabaseConnection connection,
			List<ExpressionBase> expressions,
			String statement) {

		super(prologue);
		this.connection = connection;
		this.expressions = expressions;
		this.statement = statement;
	}

	@Override
	protected void execute(Context context) {

		List<Object> values = this.expressions.stream().map(e -> e.getEvaluationObject()).collect(Collectors.toCollection(LinkedList::new));

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(connection);

			stmt = prepareParameterizedStatement(values, statement, conn);

			execute(stmt);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Database query terminated due to interruption");
		}
		finally { try {

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
