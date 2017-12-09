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
import java.sql.SQLException;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.StatementDataSource;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.expression.StringConstant;
import com.hauldata.dbpa.file.flat.TextFile;
import com.hauldata.dbpa.process.Context;

public class RunScriptTask extends Task {

	private Expression<String> source;
	DatabaseConnection connection;

	public RunScriptTask(
			Prologue prologue,
			DatabaseConnection connection,
			Expression<String> source) {

		super(prologue);
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		Path sourcePath = context.getReadPath(source.evaluate());
		context.files.assureNotOpen(sourcePath);

		// Get the script body into a string.
		// See http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file

		String body = null;
		try {
			byte[] encoded = Files.readAllBytes(sourcePath);
			body = new String(encoded, TextFile.getDefaultCharset());
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to read script file: " + message, ex);
		}

		// Execute the script body.

		DataSource dataSource = new StatementDataSource(connection, new StringConstant(body), false);

		try {
		    dataSource.executeUpdate(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Script execution terminated due to interruption");
		}
		finally {
			try { dataSource.done(context); } catch (Exception ex) {}
			try { dataSource.close(context); } catch (Exception ex) {}
		}
	}
}
