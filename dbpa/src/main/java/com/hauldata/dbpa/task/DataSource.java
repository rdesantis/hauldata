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

	protected Connection conn;
	protected Statement stmt;
	protected ResultSet rs;

	protected DataSource(DatabaseConnection connection) {
		this.connection = connection;
	}

	public abstract ResultSet getResultSet(Context context) throws SQLException;

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
	protected Object toSQL(Object value) {
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
	protected Object fromSQL(Object value) {
		if (value instanceof Date) {
			return ((Date)value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		}
		else {
			return value;
		}
	}
}
