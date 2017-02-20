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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.process.Context;

public abstract class DataStore {

	// Static methods

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
	}

	public void close(Context context) {

		if (stmt != null) try { stmt.close(); } catch (Exception ex) {}
		if (conn != null) context.releaseConnection(connection);

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
}
