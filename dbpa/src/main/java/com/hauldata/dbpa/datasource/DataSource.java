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

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.process.Context;

public abstract class DataSource extends DataStore {

	private boolean singleRow;

	protected ResultSet rs;

	protected DataSource(DatabaseConnection connection, boolean singleRow) {

		super(connection);
		this.singleRow = singleRow;
	}

	protected int getResultSetType() {
		return singleRow ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
	}

	public abstract void executeUpdate(Context context) throws SQLException, InterruptedException;

	public abstract void executeQuery(Context context) throws SQLException, InterruptedException;

	public int getColumnCount() throws SQLException {
		return rs.getMetaData().getColumnCount();
	}

	public String getColumnLabel(int column) throws SQLException {
		return rs.getMetaData().getColumnLabel(column);
	}

	public boolean next() throws SQLException, InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		return rs.next();
	}

	public Object getObject(int columnIndex) throws SQLException {
		return rs.getObject(columnIndex);
	}

	public boolean isLast() throws SQLException {
		return rs.isLast();
	}

	public void done(Context context) throws SQLException {}

	public void close(Context context) {

		if (rs != null) try { rs.close(); } catch (Exception ex) {}

		rs = null;

		super.close(context);
	}

	/**
	 * Execute a statement so that it is terminated when this thread is interrupted.
	 *
	 * @return the result of the Statement.executeUpdate() method on the statement.
	 * @throws SQLException if thrown by the Statement.executeUpdate() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected int executeUpdate(String sql) throws SQLException, InterruptedException {

		SQLStatementExecutor executor = new SQLStatementExecutor(sql);

		execute(executor);

		return executor.getResult();
	}

	private class SQLStatementExecutor extends SQLExecutor {

		private String sql;
		private int result;

		SQLStatementExecutor(String sql) {
			this.sql = sql;
		}

		@Override
		public void run() {
			try {
				result = stmt.executeUpdate(sql);
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public int getResult() throws SQLException {
			getException();
			return result;
		}
	}

	/**
	 * Execute a query so that it is terminated when this thread is interrupted.
	 *
	 * @return the result of the Statement.executeQuery() method on the statement.
	 * @throws SQLException if thrown by the Statement.executeQuery() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected ResultSet executeQuery(String sql) throws SQLException, InterruptedException {

		SQLQueryExecutor executor = new SQLQueryExecutor(sql);

		execute(executor);

		return executor.getResult();
	}

	private class SQLQueryExecutor extends SQLExecutor {

		private String sql;
		private ResultSet result;

		SQLQueryExecutor(String sql) {
			this.sql = sql;
		}

		@Override
		public void run() {
			try {
				result = stmt.executeQuery(sql);
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public ResultSet getResult() throws SQLException {
			getException();
			return result;
		}
	}

	/**
	 * Execute a prepared statement so that it is terminated when this thread is interrupted.
	 * <p>
	 * Data member stmt must contain a prepared statement.
	 *
	 * @return the result of the PreparedStatement.executeUpdate() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.execute() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected int executePreparedUpdate() throws SQLException, InterruptedException {

		SQLPreparedStatementExecutor executor = new SQLPreparedStatementExecutor();

		execute(executor);

		return executor.getResult();
	}

	private class SQLPreparedStatementExecutor extends SQLExecutor {

		private int result;

		@Override
		public void run() {
			try {
				result = ((PreparedStatement)stmt).executeUpdate();
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public int getResult() throws SQLException {
			getException();
			return result;
		}
	}

	/**
	 * Execute a prepared query so that it is terminated when this thread is interrupted.
	 * <p>
	 * Data member stmt must contain a prepared statement.
	 *
	 * @return the result of the PreparedStatement.executeQuery() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.executeQuery() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected ResultSet executePreparedQuery() throws SQLException, InterruptedException {

		SQLPreparedQueryExecutor executor = new SQLPreparedQueryExecutor();

		execute(executor);

		return executor.getResult();
	}

	private class SQLPreparedQueryExecutor extends SQLExecutor {

		private ResultSet result;

		@Override
		public void run() {
			try {
				result = ((PreparedStatement)stmt).executeQuery();
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public ResultSet getResult() throws SQLException {
			getException();
			return result;
		}
	}
}
