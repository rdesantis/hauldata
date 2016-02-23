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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.TextFile;
import com.hauldata.dbpa.process.Context;

public class RunScriptTask extends DatabaseTask {

	public RunScriptTask(
			Prologue prologue,
			Expression<String> source) {

		super(prologue);
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		Path sourcePath = context.getDataPath(source.evaluate());
		context.files.assureNotOpen(sourcePath);

		// Get the script body into a string.
		// See http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file

		String body = null;
		try {
			byte[] encoded = Files.readAllBytes(sourcePath);
			body = new String(encoded, TextFile.getCharset());		
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to read script file: " + message, ex);
		}

		// Execute the script body.

		Connection conn = null;
		Statement stmt = null;

		try {
			conn = context.getConnection();

			stmt = conn.createStatement();
			
		    executeUpdate(stmt, body);
		}
		catch (SQLException ex) {
			throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Script execution terminated due to interruption");
		}
		finally { try {

			if (stmt != null) stmt.close();
		}
		catch (SQLException ex) {
			throwDatabaseCloseFailed(ex);
		}
		finally {
			if (conn != null) context.releaseConnection();
		} }
	}

	private Expression<String> source;
}
