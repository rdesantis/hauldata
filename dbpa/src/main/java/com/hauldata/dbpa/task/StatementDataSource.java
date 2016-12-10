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

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class StatementDataSource extends DataSource {

	private Expression<String> statement;

	public StatementDataSource(
			DatabaseConnection connection,
			Expression<String> statement) {

		super(connection);
		this.statement = statement;
	}

	@Override
	public ResultSet getResultSet(Context context) throws SQLException {

		String evaluatedStatement = statement.evaluate();

		conn = context.getConnection(connection);

		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		rs = stmt.executeQuery(evaluatedStatement);

		return rs;
	}
}
