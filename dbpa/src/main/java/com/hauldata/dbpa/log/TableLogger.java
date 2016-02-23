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

package com.hauldata.dbpa.log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import com.hauldata.dbpa.process.Context;

public class TableLogger implements Logger {

	public TableLogger(Context context, String tableName) {

		this.context = context;
		this.tableName = tableName;

		prepareStatement();
		releasePreparedStatement();
	}

	@Override
	public void log(String processId, String taskId, LocalDateTime datetime, String message) {

		PreparedStatement stmt = getPreparedStatement();
		
		if (stmt != null) {
			try {
				synchronized (stmt) {
					stmt.setString(1, processId);
					stmt.setString(2, taskId);
					stmt.setTimestamp(3, Timestamp.valueOf(datetime));
					stmt.setString(4, message);
		
					stmt.executeUpdate();
				}
			}
			catch (Exception ex) {
				// No good choice but to ignore the exception.  Don't want to crash the process just because logging is broken.
			}
			
			releasePreparedStatement();
		}
	}

	/**
	 * Prepare the INSERT statement used to write log entries.
	 */
	private void prepareStatement() {

		conn = null;
		stmt = null;

		try {
			conn = context.getConnection(); 

			String statement = "INSERT INTO " + tableName + " VALUES (?,?,?,?)";

			stmt = conn.prepareStatement(statement);
		}
		catch (SQLException ex) {

			if (conn != null) {
				context.releaseConnection();
				conn = null;
			}

			throw new RuntimeException("Database log preparation failed: " + ex.getMessage());
		}
	}

	/**
	 * Get the prepared INSERT statement used to write log entries.
	 * This function is aware that the database connection for the context
	 * can change after long sleep / wake cycles.  When the connection changes,
	 * a new prepared statement is created over the new connection.
	 *    
	 * @return the prepared statement
	 */
	private PreparedStatement getPreparedStatement() {

		synchronized (this) {
			Connection currentConn = context.getConnection();
			if ((currentConn != conn) && (currentConn != null)) {

				context.releaseConnection();

				close();

				try { prepareStatement(); } catch (Exception ex) {}
			}
		}
		return stmt;
	}

	/**
	 * Release the prepared statement acquired by getPreparedStatement().
	 * This actually releases the underlying connection as required
	 * for proper task sleep / wake connection management.
	 */
	private void releasePreparedStatement() {
		context.releaseConnection();
	}

	@Override
	public void close() {
		if (stmt != null) {
			try { stmt.close(); }
			catch (SQLException e) {}
			finally { stmt = null; }
		}
	}

	private Context context;
	private String tableName;

	private Connection conn;
	private PreparedStatement stmt;
}
