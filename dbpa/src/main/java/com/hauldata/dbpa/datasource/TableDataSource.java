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

package com.hauldata.dbpa.datasource;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class TableDataSource extends DataSource {

	private Expression<String> table;

	public TableDataSource(
			DatabaseConnection connection,
			Expression<String> table,
			boolean singleRow) {

		super(connection, singleRow);
		this.table = table;
	}

	@Override
	public void executeUpdate(Context context) throws SQLException {
		throw new RuntimeException("Internal error: call to TableDataSource.executeUpdate(Context)");
	}

	@Override
	public ResultSet executeQuery(Context context) throws SQLException, InterruptedException {

		String statement = "SELECT * FROM " + table.evaluate();

		conn = context.getConnection(connection);

		stmt = conn.createStatement(getResultSetType(), ResultSet.CONCUR_READ_ONLY);

		rs = executeQuery(stmt, statement);

		return rs;
	}
}
