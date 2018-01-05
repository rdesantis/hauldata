/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.datasource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.process.Context;

public class ParameterizedStatementDataSource extends DataSource {

	private List<ExpressionBase> expressions;
	private String statement;

	public ParameterizedStatementDataSource(
			DatabaseConnection connection,
			List<ExpressionBase> expressions,
			String statement,
			boolean singleRow) {

		super(connection, singleRow);
		this.expressions = expressions;
		this.statement = statement;
	}

	@Override
	public void executeUpdate(Context context) throws SQLException, InterruptedException {

		prepareStatement(context);

		executePreparedUpdate();
	}

	@Override
	public void executeQuery(Context context) throws SQLException, InterruptedException {

		prepareStatement(context);

		rs = executePreparedQuery();
	}

	private void prepareStatement(Context context) throws SQLException {

		conn = context.getConnection(connection);

		stmt = conn.prepareStatement(statement, getResultSetType(), ResultSet.CONCUR_READ_ONLY);

		PreparedStatement prepared = (PreparedStatement)stmt;

		int parameterIndex = 1;
		for (ExpressionBase expression : expressions) {
			prepared.setObject(parameterIndex++, toSQL(expression.getEvaluationObject(), expression.getType()));
		}
	}
}
