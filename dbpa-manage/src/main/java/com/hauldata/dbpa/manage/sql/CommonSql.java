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

package com.hauldata.dbpa.manage.sql;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;

import javax.naming.NameNotFoundException;

public class CommonSql {

	private static final String notFoundMessageStem = "%s not found: ";

	public static String getNotFoundMessageStem(String entityName) {
		return String.format(notFoundMessageStem, entityName);
	}

	/**
	 * Use reflection to initialize data members from like-named members with "_" suffix
	 * substituting tablePrefix where "%1$s" appears in the "_" member.
	 * @param tablePrefix
	 */
	protected CommonSql(String tablePrefix) {

		Iterator<String> nameIterator = Arrays.stream(this.getClass().getDeclaredFields()).map(f -> f.getName()).filter(n -> n.endsWith("_")).iterator();

		while (nameIterator.hasNext()) {
			try {
				String name_ = nameIterator.next();
				String name = name_.substring(0, name_.length() - 1);

				Field field_ = this.getClass().getDeclaredField(name_);
				Field field = this.getClass().getDeclaredField(name);

				String value_ = (String)field_.get(this);

				String value = String.format(value_, tablePrefix);

				field.set(this, value);
			}
			catch (Exception ex) {}
		}
	}

	/**
	 * @return "ASC" is ascending is true, otherwise "DESC"
	 */
	public static String orderBy(boolean ascending) {
		return ascending ? "ASC" : "DESC";
	}

	/**
	 * For a query that returns a single integer, this function returns that integer plus 1.
	 * <p>
	 * This is intended for generating a new entity ID, in which case it should be called
	 * from code that is protected against concurrent operations.
	 *
	 * @param conn is the database connection to use for the query
	 * @param selectLastId is the query
	 * @return the result of the query plus 1, or 1 if the query returns SQL NULL.
	 * @throws SQLException
	 */
	public static int getNextId(Connection conn, String selectLastId) throws SQLException {

		int nextId = -1;

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			rs = stmt.executeQuery(selectLastId);

			rs.next();

			nextId = rs.getInt(1) + 1;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return nextId;
	}

	/**
	 * For a query that takes a single string parameter and returns a single integer, this function returns that integer.
	 * Optionally throws an exception if the string is not found.
	 * <p>
	 * This is intended for getting the entity ID of an existing entity given its name.
	 *
	 * @param conn is the database connection to use for the query
	 * @param selectId is the query with one ? parameter
	 * @param name is the value to substitute for the single query parameter
	 * @param entityName if not null and the query returns an empty result set, contains a string to use in the message
	 * included with a NameNotFoundException thrown.  If null, no exception is thrown.
	 * @return the result of the query or -1 if the query returns an empty result set.
	 * @throws NameNotFoundException if the query returns an empty result set and entityName is not null.
	 * @throws SQLException
	 */
	public static int getId(Connection conn, String selectId, String name, String entityName) throws SQLException, NameNotFoundException {

		int id = -1;

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(selectId, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, name);

			rs = stmt.executeQuery();

			if (rs.next()) {
				id = rs.getInt(1);
			}
			else if (entityName != null) {
				throw new NameNotFoundException(getNotFoundMessageStem(entityName) + name);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return id;
	}

	public static int getId(Connection conn, String selectId, String name) throws SQLException {
		try {
			return getId(conn, selectId, name, null);
		}
		catch (NameNotFoundException ex) {
			// This exception will never happen with this form of the call.
			return -1;
		}
	}

	/**
	 * Execute a SQL statement after setting one integer parameter.
	 * <p>
	 * This is intended for executing a statement against an entity given its ID.
	 *
	 * @param conn is the connection
	 * @param sql is the SQL to execute with one ? parameter
	 * @param id is the integer value to substitute for the ? parameter
	 * @return the result of PreparedStatement.executeUpdate()
	 * @throws SQLException
	 */
	public static int execute(
			Connection conn,
			String sql,
			int id) throws SQLException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sql);

			stmt.setInt(1, id);

			return stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	/**
	 * Execute a SQL statement after: a string textual substitution, a string parameter substitution,
	 * and an integer parameter substitution.
	 * <p>
	 * This is intended for updating a string column of an entity identified by its ID,
	 * where the column name is provided as an argument.
	 *
	 * @param conn is the connection
	 * @param sql is the SQL to execute with one %1$s placeholder and two ? parameters
	 * @param columnName is the string to substituted into the %1$s placeholder
	 * @param value is the string value to substitute for the first ? parameter
	 * @param id is the integer value to substitute for the second ? parameter
	 * @return the result of PreparedStatement.executeUpdate()
	 * @throws SQLException
	 */
	public static int execute(
			Connection conn,
			String sql,
			String columnName,
			String value,
			int id) throws SQLException {

		PreparedStatement stmt = null;

		try {
			sql = String.format(sql, columnName);

			stmt = conn.prepareStatement(sql);

			stmt.setString(1, value);
			stmt.setInt(2, id);

			return stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	/**
	 * Execute a SQL statement after: a string textual substitution, an integer parameter substitution,
	 * and another integer parameter substitution.
	 * <p>
	 * This is intended for updating an integer column of an entity identified by its ID,
	 * where the column name is provided as an argument.
	 *
	 * @param conn is the connection
	 * @param sql is the SQL to execute with one %1$s placeholder and two ? parameters
	 * @param columnName is the string to substituted into the %1$s placeholder
	 * @param value is the integer value to substitute for the first ? parameter
	 * @param id is the integer value to substitute for the second ? parameter
	 * @return the result of PreparedStatement.executeUpdate()
	 * @throws SQLException
	 */
	public static int execute(
			Connection conn,
			String sql,
			String columnName,
			int value,
			int id) throws SQLException {

		PreparedStatement stmt = null;

		try {
			sql = String.format(sql, columnName);

			stmt = conn.prepareStatement(sql);

			stmt.setInt(1, value);
			stmt.setInt(2, id);

			return stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	/**
	 * Execute a SQL UPDATE statement setting a field to the current datetime after
	 * an integer parameter substitution.
	 * <p>
	 * This is intended for updating a datetime column of an entity identified by its ID.
	 *
	 * @param conn is the connection
	 * @param sql is the SQL to execute with two ? parameters, the first being for
	 * a datetime column value in a SET clause and the second for an integer ID
	 * value in a WHERE clause.
	 * @param id is the integer value to substitute for the second ? parameter
	 * @return the result of PreparedStatement.executeUpdate()
	 * @throws SQLException
	 */
	public static int executeUpdateNow(
			Connection conn,
			String sql,
			int id) throws SQLException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sql);

			stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
			stmt.setInt(2, id);

			return stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}
}
