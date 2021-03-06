/*
 * Copyright (c) 2017, 2019, Ronald DeSantis
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
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.process.Context;

public class StatementDataTarget extends DataTarget {

	Expression<String> statement;

	public StatementDataTarget(
			DatabaseConnection connection,
			Expression<Integer> batchSize,
			Expression<String> statement) {

		super(connection, batchSize);
		this.statement = statement;
	}

	@Override
	public void prepareStatement(Context context, Columns columns) throws SQLException {

		String sql = statement.evaluate();

		prepareStatement(context, sql);
	}
}
