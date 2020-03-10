/*
 * Copyright (c) 2017, Ronald DeSantis
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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.VariableType;

public abstract class DataStore {

	// Static methods

	/**
	 * Return a value retrieved from ExpressionBase.getEvaluationObject() converted if necessary
	 * to an object type acceptable to PreparedStatement.setObject(Object) on all database systems.
	 */
	public static Object toSQL(Object value, VariableType type) {
		if (value == null) {
			return null;
		}
		else if (type == VariableType.BIT) {
			return (Boolean)((Integer)value == 0 ? false : true);
		}
		else if (type == VariableType.DATETIME) {
			return Timestamp.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant());
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
		if (value instanceof Boolean) {
			return (Integer)((Boolean)value ? 1 : 0);
		}
		else if (value instanceof Date) {
			return ((Date)value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		}
		else {
			return value;
		}
	}

	public static void throwDatabaseExecutionFailed(Exception ex) {
		throwDatabaseFailed("execution", ex);
	}

	public static void throwDatabaseCloseFailed(Exception ex) {
		throwDatabaseFailed("close", ex);
	}

	private static void throwDatabaseFailed(String operation, Exception ex) {
		String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
		throw new RuntimeException("Database statement " + operation + " failed: " + message, ex);
	}

	// End of static methods

	protected DatabaseConnection connection;

	protected Connection conn;
	protected Statement stmt;

	protected DataStore(DatabaseConnection connection) {
		this.connection = connection;

		this.conn = null;
		this.stmt = null;
	}

	protected void getConnection(Context context) {

		conn = context.getConnection(connection);
		if (conn == null) {
			throw new RuntimeException("Database connection properties are not defined");
		}
	}

	public void close(Context context) {

		if (stmt != null) try { stmt.close(); } catch (Exception ex) {}
		if (conn != null) context.releaseConnection(connection, conn);

		stmt = null;
		conn = null;
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

	protected abstract class SQLExecutor implements Runnable {

		protected SQLException ex;

		public SQLExecutor() {
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

	/**
	 * Execute a statement so that it is terminated when this thread is interrupted.
	 *
	 * @return the result of the Statement.executeUpdate() method on the statement.
	 * @throws SQLException if thrown by the Statement.executeUpdate() method.
	 * @throws InterruptedException if this thread was interrupted while the statement was in progress,
	 * in which case the statement execution is attempted to be terminated subject
	 * to the limitations specified on Statement.cancel().
	 */
	public int executeUpdate(String sql) throws SQLException, InterruptedException {

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

		SQLBatchExecutor executor = new SQLBatchExecutor();

		execute(executor);

		return executor.getResult();
	}

	private class SQLBatchExecutor extends SQLExecutor {

		private int[] result;

		@Override
		public void run() {
			try {
				result = stmt.executeBatch();
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
