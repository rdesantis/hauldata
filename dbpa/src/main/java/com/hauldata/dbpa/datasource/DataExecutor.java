/*
 * Copyright (c) 2020, Ronald DeSantis
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

import java.sql.SQLException;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.process.Context;

public class DataExecutor extends DataStore {

	public DataExecutor(DatabaseConnection connection) {
		super(connection);
	}

	public String getDialect(Context context) {
		DatabaseConnection resolvedConnection = context.resolveConnection(connection);
		String dialect = resolvedConnection.getProperties().getProperty("dialect");
		if (dialect != null) {
			dialect = dialect.toUpperCase();
		}
		else {
			String driver = resolvedConnection.getProperties().getProperty("driver");
			if ((driver != null) && driver.toUpperCase().contains("SQLSERVER")) {
				dialect = "T-SQL";
			}
		}
		return dialect;
	}

	/**
	 * createStatement(Context) must be called before calling addBatch(String)
	 * or any base class methods.
	 */
	public void createStatement(Context context) throws SQLException {

		getConnection(context);

		stmt = conn.createStatement();
	}

	public void addBatch(String sql) throws SQLException {
		stmt.addBatch(sql);
	}
}
