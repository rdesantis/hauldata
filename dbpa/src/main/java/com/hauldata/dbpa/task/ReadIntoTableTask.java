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

import java.io.IOException;
import java.util.List;

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.ReadHeaders;
import com.hauldata.dbpa.file.ReadPage;
import com.hauldata.dbpa.process.Context;

public class ReadIntoTableTask extends FileTask {

	private PageIdentifierExpression page;
	private ReadHeaderExpressions headers;
	private ColumnExpressions columns;
	private DatabaseConnection connection;
	private Expression<String> table;
	private Expression<String> prefix;

	public ReadIntoTableTask(
			Prologue prologue,
			PageIdentifierExpression page,
			ReadHeaderExpressions headers,
			ColumnExpressions columns,
			DatabaseConnection connection,
			Expression<String> table,
			Expression<String> prefix) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.columns = columns;
		this.connection = connection;
		this.table = table;
		this.prefix = prefix;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, false);
		ReadHeaders headers = this.headers.evaluate();
		String table = this.table.evaluate();
		String prefix = (this.prefix != null) ? this.prefix.evaluate() : null;

		context.files.assureNotOpen(page.getPath());
		try {
			ReadPage readPage = page.read(context.files, headers);
			Columns columns = this.columns.evaluate(readPage.getReadHeaders());
			List<String> captions = columns.getCaptions();

			StringBuilder statement = new StringBuilder();
			if (prefix != null) {
				statement.append(prefix).append(" ");
			}

			statement.append("INSERT INTO ").append(table).append(" ");

			if (headers.toMetadata()) {
				statement.append("(");

				String separator = "";
				for (String caption : captions) {
					if (caption.length() == 0) {
						throw new RuntimeException("File has a blank column header - not allowed when headers are not explicitly provided");
					}
					statement.append(separator).append(caption);
					separator = ", ";
				}
				statement.append(") ");
			}

			statement.append("VALUES (");

			String separator = "";
			for (int i = 0; i < captions.size(); ++i) {
				statement.append(separator).append("?");
				separator = ",";
			}

			statement.append(")");

			readIntoStatement(context, connection, readPage, columns, statement.toString());
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
	}
}
