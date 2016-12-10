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

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.ReadPage;
import com.hauldata.dbpa.file.WritePage;
import com.hauldata.dbpa.process.Context;

public abstract class FileTask extends DatabaseTask {

	public FileTask(Prologue prologue) {
		super(prologue);
	}

	protected void write(
			Context context,
			DataSource dataSource,
			WritePage page) {

		try {
			ResultSet rs = dataSource.executeQuery(context);

			page.write(rs);

			dataSource.done(context);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File write terminated due to interruption");
		}
		finally {
			dataSource.close(context);
		}
	}

	protected void readIntoStatement(
			Context context,
			DatabaseConnection connection,
			ReadPage page,
			Columns columns,
			String statement) {
		
		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(connection);

			stmt = conn.prepareStatement(statement);

			page.read(columns, stmt);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("File read terminated due to interruption");
		}
		finally { try {
			if (stmt != null) stmt.close();
		}
		catch (SQLException ex) {
			throwDatabaseCloseFailed(ex);
		}
		finally {
			try { page.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(connection);
		} }
	}
}
