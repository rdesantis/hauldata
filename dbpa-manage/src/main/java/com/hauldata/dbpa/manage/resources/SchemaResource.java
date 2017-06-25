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

package com.hauldata.dbpa.manage.resources;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.SchemaException;
import com.hauldata.dbpa.process.Context;

@Path("/schema")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SchemaResource {

	public SchemaResource() {}

	/**
	 * Create the set of database tables used to store job configurations
	 * and run results.
	 * @throws SQLException if schema creation fails due to a database issue
	 * @throws JobException due to a job manager issue
	 */
	@PUT
	@Timed
	public void create() throws SQLException, SchemaException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();

		Connection conn = null;
		Statement stmt = null;

		try {
			// Create the tables in the schema.

			conn = context.getConnection(null);

			stmt = conn.createStatement();

			stmt.executeUpdate(manager.getJobSql().createTable);
		    stmt.executeUpdate(manager.getArgumentSql().createTable);
		    stmt.executeUpdate(manager.getScheduleSql().createTable);
		    stmt.executeUpdate(manager.getJobScheduleSql().createTable);
		    stmt.executeUpdate(manager.getRunSql().createTable);
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	/**
	 * Drop the job-related tables.
	 * @throws SQLException if schema removal fails due to a database issue
	 * @throws JobException due to a job manager issue
	 */
	@DELETE
	@Timed
	public void delete() throws SQLException, SchemaException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();

		Connection conn = null;
		Statement stmt = null;

		try {
			// Attempt to drop the tables in the schema.
			// They may not exist; that is not an error.

			conn = context.getConnection(null);

			stmt = conn.createStatement();

			tryExecuteUpdate(stmt, manager.getJobSql().dropTable);
			tryExecuteUpdate(stmt, manager.getArgumentSql().dropTable);
			tryExecuteUpdate(stmt, manager.getScheduleSql().dropTable);
			tryExecuteUpdate(stmt, manager.getJobScheduleSql().dropTable);
			tryExecuteUpdate(stmt, manager.getRunSql().dropTable);
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	/**
	 * Execute a SQL statement; return true if it executed without exception, false if SQLException occurred.
	 * @param stmt is a Statement created for the connection
	 * @param sql is the SQL to execute.
	 */
	private boolean tryExecuteUpdate(Statement stmt, String sql) {
		try {
			stmt.executeUpdate(sql);
		}
		catch (SQLException e) {
			return false;
		}
		return true;
	}

	/**
	 * Confirm the existence of all the job-related tables with correct columns
	 * @return true if all tables exist correctly, false otherwise
	 * @throws RuntimeException if required properties to set up the connection were not provided,
	 * or if the connection could not be established.
	 * @throws SQLException if an unexpected SQL error occurred; non-existent or incorrect table
	 * is NOT an unexpected error.
	 */
	@GET
	@Timed
	public boolean confirm() throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();

		Connection conn = null;
		Statement stmt = null;

		boolean allTablesExistAsRequired;
		try {
			conn = context.getConnection(null);

			stmt = conn.createStatement();

			// Need a database-neutral way to test if a table exists with the required columns.
			// See http://stackoverflow.com/questions/1227921/portable-sql-to-determine-if-a-table-exists-or-not
			// Run a statement that does nothing but will fail if the table doesn't exist as required.

			allTablesExistAsRequired =
					trySelectNoRows(stmt, manager.getJobSql().selectAllColumns) &&
					trySelectNoRows(stmt, manager.getArgumentSql().selectAllColumns) &&
					trySelectNoRows(stmt, manager.getScheduleSql().selectAllColumns) &&
					trySelectNoRows(stmt, manager.getJobScheduleSql().selectAllColumns) &&
					trySelectNoRows(stmt, manager.getRunSql().selectAllColumns);
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null);
		}

		return allTablesExistAsRequired;
	}

	private boolean trySelectNoRows(Statement stmt, String selectAllColumns) {

		String selectAllColumnsNoRows = selectAllColumns + " WHERE 0=1";

		return tryExecuteQuery(stmt, selectAllColumnsNoRows);
	}

	/**
	 * Execute a SQL query; return true if it executed without exception, false if SQLException occurred.
	 * @param stmt is a Statement created for the connection
	 * @param sql is the SQL to execute.
	 */
	private boolean tryExecuteQuery(Statement stmt, String sql) {
		try {
			stmt.executeQuery(sql);
		}
		catch (SQLException e) {
			return false;
		}
		return true;
	}
}
