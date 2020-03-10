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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.process.Context;

public abstract class DataTarget extends DataStore {

	private Expression<Integer> batchSizeExpression;

	private final int batchSizeMaxDefault = 1000;
	private Integer batchSizeMax;
	private int batchSize;

	public DataTarget(DatabaseConnection connection, Expression<Integer> batchSizeExpression) {
		super(connection);
		this.batchSizeExpression = batchSizeExpression;
	}

	public abstract void prepareStatement(Context context, SourceHeaders headers, Columns columns) throws SQLException;

	protected void prepareStatement(Context context, String sql) throws SQLException {

		getConnection(context);

		stmt = conn.prepareStatement(sql);

		batchSizeMax = (batchSizeExpression != null) ? batchSizeExpression.evaluate() : batchSizeMaxDefault;
		batchSize = 0;
	}

	public int getParameterCount() throws SQLException {
		return ((PreparedStatement)stmt).getParameterMetaData().getParameterCount();
	}

	public int getParameterType(int parameterIndex) throws SQLException {
		return ((PreparedStatement)stmt).getParameterMetaData().getParameterType(parameterIndex);
	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
		((PreparedStatement)stmt).setObject(parameterIndex, x);
	}

	public void addBatch() throws SQLException, InterruptedException {
		((PreparedStatement)stmt).addBatch();

		++batchSize;
		if ((batchSizeMax != null) && (batchSizeMax <= batchSize)) {
			executeBatch();
			batchSize = 0;
		}
	}
}
