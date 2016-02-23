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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class DatabaseTask extends Task {

	public DatabaseTask(Prologue prologue) {
		super(prologue);
	}

	/**
	 * Create a Statement whose result set will return an indeterminate number of rows.
	 */
	protected Statement createStatement(Connection conn) throws SQLException {
		return conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	/**
	 * Create a Statement whose result set will be tested to confirm that it returns exactly one row.
	 */
	protected Statement createOneRowStatement(Connection conn) throws SQLException {
		return conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	private PreparedStatement prepareParameterizedStatement(List<Object> values, String statement, Connection conn, boolean willCheckForOneRow) throws SQLException {

		PreparedStatement stmt = conn.prepareStatement(
				statement,
				willCheckForOneRow ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);

		int parameterIndex = 1;
		for (Object value : values) {
			stmt.setObject(parameterIndex++, value);
		}

		return stmt;
	}

	/**
	 * Prepare a PreparedStatement whose result set will return an indeterminate number of rows.
	 */
	protected PreparedStatement prepareParameterizedStatement(List<Object> values, String statement, Connection conn) throws SQLException {

		return prepareParameterizedStatement(values, statement, conn, false);
	}

	/**
	 * Prepare a PreparedStatement whose result set will be tested to confirm that it returns exactly one row.
	 */
	protected PreparedStatement prepareOneRowParameterizedStatement(List<Object> values, String statement, Connection conn) throws SQLException {

		return prepareParameterizedStatement(values, statement, conn, true);
	}

	/**
	 * Execute a statement so that it is terminated when this thread is interrupted.
	 * @param stmt is the statement to execute.
	 * @return the result of the Statement.executeUpdate() method on the statement.
	 * @throws SQLException if thrown by the Statement.executeUpdate() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected int executeUpdate(Statement stmt, String sql) throws SQLException, InterruptedException {

		SQLStatementExecutor executor = new SQLStatementExecutor(stmt, sql);

		execute(executor);
		
		return executor.getResult();
	}

	/**
	 * Execute a query so that it is terminated when this thread is interrupted.
	 * @param stmt is the query statement to execute.
	 * @return the result of the Statement.executeQuery() method on the statement.
	 * @throws SQLException if thrown by the Statement.executeQuery() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected ResultSet executeQuery(Statement stmt, String sql) throws SQLException, InterruptedException {

		SQLQueryExecutor executor = new SQLQueryExecutor(stmt, sql);

		execute(executor);
		
		return executor.getResult();
	}

	/**
	 * Execute a prepared statement so that it is terminated when this thread is interrupted.
	 * @param stmt is the statement to execute.
	 * @return the result of the PreparedStatement.execute() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.execute() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected boolean execute(PreparedStatement stmt) throws SQLException, InterruptedException {

		SQLPreparedStatementExecutor executor = new SQLPreparedStatementExecutor(stmt);

		execute(executor);
		
		return executor.getResult();
	}

	/**
	 * Execute a prepared query so that it is terminated when this thread is interrupted.
	 * @param stmt is the query statement to execute.
	 * @return the result of the PreparedStatement.executeQuery() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.executeQuery() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	protected ResultSet executeQuery(PreparedStatement stmt) throws SQLException, InterruptedException {

		SQLPreparedQueryExecutor executor = new SQLPreparedQueryExecutor(stmt);

		execute(executor);
		
		return executor.getResult();
	}

	protected void execute(SQLExecutor executor) throws SQLException, InterruptedException {

		Thread executorThread = new Thread(executor); 

		executorThread.start();

		try {
			executorThread.join();
		}
		catch (InterruptedException iex) {
			executor.cancel();
			throw iex;
		}
	}

	private abstract class SQLExecutor implements Runnable {

		protected Statement stmt;
		protected SQLException ex;

		public SQLExecutor(Statement stmt) {
			this.stmt = stmt;
			ex = null;
		}

		public void cancel() throws SQLException {
			stmt.cancel();
		}

		protected void getException() throws SQLException {
			if (ex != null) {
				throw ex;
			}
		}
	}

	private class SQLStatementExecutor extends SQLExecutor {

		private String sql;
		private int result;

		SQLStatementExecutor(Statement stmt, String sql) {
			super(stmt);
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

	private class SQLQueryExecutor extends SQLExecutor {

		private String sql;
		private ResultSet result;

		SQLQueryExecutor(Statement stmt, String sql) {
			super(stmt);
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

	private class SQLPreparedStatementExecutor extends SQLExecutor {

		private boolean result;

		SQLPreparedStatementExecutor(PreparedStatement stmt) {
			super(stmt);
		}

		@Override
		public void run() {
			try {
				result = ((PreparedStatement)stmt).execute();
			} catch (SQLException ex) {
				this.ex = ex;
			}
		}

		public boolean getResult() throws SQLException {
			getException();
			return result;
		}
	}

	private class SQLPreparedQueryExecutor extends SQLExecutor {

		private ResultSet result;

		SQLPreparedQueryExecutor(PreparedStatement stmt) {
			super(stmt);
		}

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

	// Common exceptions

	protected void throwDatabaseExecutionFailed(Exception ex) {
		throwDatabaseFailed("execution", ex);
	}

	protected void throwDatabaseCloseFailed(Exception ex) {
		throwDatabaseFailed("close", ex);
	}

	private void throwDatabaseFailed(String operation, Exception ex) {
		String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
		throw new RuntimeException("Database statement " + operation + " failed: " + message, ex);
	}

}
