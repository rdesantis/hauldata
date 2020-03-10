/*
 * Copyright (c) 2016, 2020, Ronald DeSantis
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import com.hauldata.dbpa.datasource.DataExecutor;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.flat.TxtFile;
import com.hauldata.dbpa.process.Context;

public class RunScriptTask extends Task {

	private Expression<String> source;
	DataExecutor executor;

	public RunScriptTask(
			Prologue prologue,
			DataExecutor executor,
			Expression<String> source) {

		super(prologue);
		this.executor = executor;
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		Path sourcePath = context.getReadPath(source.evaluate());
		context.files.assureNotOpen(sourcePath);

		try {
			executor.createStatement(context);

			String dialect = executor.getDialect(context);
			if ("T-SQL".equals(dialect) || "TRANSACT-SQL".equals(dialect)) {
				executeTSQL(context, executor, sourcePath);
			}
			else {
				executeGeneric(executor, sourcePath);
			}
		}
		catch (SQLException ex) {
			DataExecutor.throwDatabaseExecutionFailed(ex);
		}
		catch (IOException ex) {
			throw new RuntimeException("Error attempting to read script file: " + ex.toString());
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Script execution terminated due to interruption");
		}
		finally {
			executor.close(context);
		}
	}

	private void executeTSQL(Context context, DataExecutor executor, Path sourcePath) throws IOException, SQLException, InterruptedException {

		TxtFile sourcePage = new TxtFile(context.files, sourcePath, null);
		try {

			sourcePage.open();
			sourcePage.setOpen(true);

			String batch = "";
			String line;
			do {
				line = (String)sourcePage.readColumn(1);
				boolean endOfBatch = (line == null);

				if (!endOfBatch) {
					sourcePage.readColumn(2);

					// A line that starts with GO defines the end of a batch to be executed.

					String leading = line.trim().toUpperCase();
					if (leading.startsWith("GO")) {
						String trailing = line.substring(2).trim();

						if (trailing.isEmpty() || trailing.startsWith("--")) {
							endOfBatch = true;
						}
						else {
							throw new RuntimeException("GO can only be followed by a comment at line " + String.valueOf(sourcePage.lineno()));
						}
					}
				}

				if (endOfBatch) {
					if (!batch.isEmpty()) {
						executor.addBatch(batch);
						executor.executeBatch();
					}
					batch = "";
				}
				else {
					batch += line;
					batch += "\n";
				}
			} while (line != null);
		}
		finally {
			if (sourcePage.isOpen()) { try { sourcePage.close(); } catch (Exception ex) {} }
		}
	}

	private void executeGeneric(DataExecutor executor, Path sourcePath) throws IOException, SQLException, InterruptedException  {

		// Get the script body into a string.
		// See http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file

		byte[] encoded = Files.readAllBytes(sourcePath);
		String body = new String(encoded, TxtFile.getDefaultCharset());

		// Execute the script body.

		executor.executeUpdate(body);
	}
}
