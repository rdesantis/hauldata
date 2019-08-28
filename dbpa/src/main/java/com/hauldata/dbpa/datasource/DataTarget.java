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
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourceOptions;
import com.hauldata.dbpa.process.Context;

public abstract class DataTarget extends DataStore {

	private final Integer batchSizeMaxDefault = 1000;
	private Integer batchSizeMax;
	private int batchSize;

	public DataTarget(DatabaseConnection connection) {
		super(connection);
	}

	public abstract void prepareStatement(Context context, SourceOptions options, SourceHeaders headers, Columns columns) throws SQLException;

	protected void prepareStatement(Context context, SourceOptions options, String sql) throws SQLException {

		batchSizeMax = (options != null) ? options.getBatchSize() : batchSizeMaxDefault;
		batchSize = 0;

		getConnection(context);

		stmt = conn.prepareStatement(sql);
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

	/**
	 * Execute a batch so that it is terminated when this thread is interrupted.
	 * <p>
	 * Data member stmt must contain a prepared statement.
	 *
	 * @return the result of the PreparedStatement.executeBatch() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.executeBatch() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	public int[] executeBatch() throws SQLException, InterruptedException {

		if (0 < batchSize) {
			SQLBatchExecutor executor = new SQLBatchExecutor();

			execute(executor);

			return executor.getResult();
		}
		else {
			return new int[0];
		}
	}

	private class SQLBatchExecutor extends SQLExecutor {

		private int[] result;

		@Override
		public void run() {
			try {
				result = ((PreparedStatement)stmt).executeBatch();
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public int[] getResult() throws SQLException {
			getException();
			return result;
		}
	}
}
