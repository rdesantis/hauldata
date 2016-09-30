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
import java.sql.SQLException;
import java.sql.Statement;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class RunStatementTask extends DatabaseTask {

	private DatabaseConnection connection;
	private Expression<String> statement;

	public RunStatementTask(
			Prologue prologue,
			DatabaseConnection connection,
			Expression<String> statement) {
		
		super(prologue);
		this.connection = connection;
		this.statement =  statement;
	}

	@Override
	protected void execute(Context context) {

		Connection conn = null;
		Statement stmt = null;

		try {
			conn = context.getConnection(connection);

			stmt = conn.createStatement();
			
		    executeUpdate(stmt, statement.evaluate());
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
