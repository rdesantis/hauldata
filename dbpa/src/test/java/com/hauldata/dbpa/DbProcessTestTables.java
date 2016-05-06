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

package com.hauldata.dbpa;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.hauldata.dbpa.process.Context;

public class DbProcessTestTables {

	private static boolean isAssured = false;

	public static void assureExist(Context context) {
		if (isAssured) return;

		Connection conn = null;
		try {
			conn = context.getConnection();

			if (!tablesExist(conn)) {
				createThingsTable(conn);
				createImportTargetTable(conn);
				createLotsOfDataTable(conn);
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex.getLocalizedMessage());
		}
		finally {
			if (conn != null) context.releaseConnection();
		}

		isAssured = true;
	}

	private static boolean tablesExist(Connection conn) {

		// Need a database-neutral way to test the existence of the tables.
		// See http://stackoverflow.com/questions/1227921/portable-sql-to-determine-if-a-table-exists-or-not
		// Run a statement that does nothing but will fail if the table doesn't exist.
		// Assume an exception means none of the tables don't exist.

		boolean exists = true;
		try {
			String sql = "DELETE FROM test.things WHERE 0=1";
			execute(conn, sql);
		} catch (SQLException e) {
			exists = false;
		}
		return exists;
	}

	private static void createThingsTable(Connection conn) throws SQLException {

		String sql =
				"CREATE TABLE test.things (" +
				"	name VARCHAR(25) NOT NULL," +
				"	description VARCHAR(100) DEFAULT NULL," +
				"	size INT DEFAULT NULL," +
				"	PRIMARY KEY(name)" +
				");" +
				"INSERT INTO test.things VALUES " +
				"('fred', 'some guy named fred', 1)," +
				"('Fido', 'Fred''s dog', 2)," +
				"('Some Thing', 'whatever', 3)," +
				"('ethel', 'a nice lady', 12)," +
				"('has bad chars', '[“in quotes”]', 123456)," +
				"('has blank description', '', 55555);";

		execute(conn, sql);
	}

	private static void createImportTargetTable(Connection conn) throws SQLException {

		String sql =
				"CREATE TABLE test.importtarget (" +
				"	number INT DEFAULT NULL," +
				"	word VARCHAR(45) DEFAULT NULL" +
				")";

		execute(conn, sql);
	}

	private static void createLotsOfDataTable(Connection conn) throws SQLException {

		StringBuilder sql = new StringBuilder();
		sql.append(
				"CREATE TABLE test.lotsofdata (" +
				"	data_id INT NOT NULL," +
				"	description VARCHAR(45) DEFAULT NULL," +
				"	PRIMARY KEY(data_id)" +
				");" +
				"INSERT INTO test.lotsofdata VALUES "
				);

		final int maxDataId = 99;
		int i = 1;
		boolean done = false;
		do {
			sql.append(String.format("(%d, 'first'), (%d, 'second'), (%d, 'third')", i, i + 1, i + 2));
			if ((i += 3) < maxDataId) {
				sql.append(",");
			}
			else {
				sql.append(";");
				done = true;
			}
		} while (!done);

		execute(conn, sql.toString());
	}

	private static void execute(Connection conn, String sql) throws SQLException {
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			
			stmt.executeUpdate(sql);
		}
		finally {
			if (stmt != null) stmt.close();
		}
	}
}
