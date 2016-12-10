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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.process.Context;

public abstract class DataSource {

	protected DatabaseConnection connection;
	private boolean singleRow;

	protected Connection conn;
	protected Statement stmt;
	protected ResultSet rs;

	protected DataSource(
			DatabaseConnection connection,
			boolean singleRow) {
		this.connection = connection;
		this.singleRow = singleRow;
	}

	protected int getResultSetType() {
		return singleRow ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
	}

	public abstract void executeUpdate(Context context) throws SQLException, InterruptedException;

	public abstract ResultSet executeQuery(Context context) throws SQLException, InterruptedException;

	public void done(Context context) throws SQLException {}

	public void close(Context context) {

		if (rs != null) try { rs.close(); } catch (Exception ex) {}
		if (stmt != null) try { stmt.close(); } catch (Exception ex) {}
		if (conn != null) context.releaseConnection(connection);

		rs = null;
		stmt = null;
		conn = null;
	}

	/**
	 * Return a value retrieved from ExpressionBase.getEvaluationObject() converted if necessary
	 * to an object type acceptable to PreparedStatement.setObject(Object) on all database systems.
	 */
	public static Object toSQL(Object value) {
		if (value instanceof LocalDateTime) {
			return Date.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant());
		}
		else {
			return value;
		}
	}

	/**
	 * Return a value retrieved from PreparedStatement.getObject() converted if necessary
	 * to an object type acceptable to VariableBase.setValueObject(Object).
	 */
	public static Object fromSQL(Object value) {
		if (value instanceof Date) {
			return ((Date)value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		}
		else {
			return value;
		}
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
	public static int executeUpdate(Statement stmt, String sql) throws SQLException, InterruptedException {

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
	public static ResultSet executeQuery(Statement stmt, String sql) throws SQLException, InterruptedException {

		SQLQueryExecutor executor = new SQLQueryExecutor(stmt, sql);

		execute(executor);

		return executor.getResult();
	}

	/**
	 * Execute a prepared statement so that it is terminated when this thread is interrupted.
	 * @param stmt is the statement to execute.
	 * @return the result of the PreparedStatement.executeUpdate() method on the statement.
	 * @throws SQLException if thrown by the PreparedStatement.execute() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	public static int executeUpdate(PreparedStatement stmt) throws SQLException, InterruptedException {

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
	public static ResultSet executeQuery(PreparedStatement stmt) throws SQLException, InterruptedException {

		SQLPreparedQueryExecutor executor = new SQLPreparedQueryExecutor(stmt);

		execute(executor);

		return executor.getResult();
	}

	private static void execute(SQLExecutor executor) throws SQLException, InterruptedException {

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

	private static abstract class SQLExecutor implements Runnable {

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

	private static class SQLStatementExecutor extends SQLExecutor {

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

	private static class SQLQueryExecutor extends SQLExecutor {

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

	private static class SQLPreparedStatementExecutor extends SQLExecutor {

		private int result;

		SQLPreparedStatementExecutor(PreparedStatement stmt) {
			super(stmt);
		}

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

	private static class SQLPreparedQueryExecutor extends SQLExecutor {

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
}
