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

import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.ReadPage;
import com.hauldata.dbpa.process.Context;

public class LoadIntoStatementTask extends FileTask {

	private PageIdentifierExpression page;
	private ColumnExpressions columns;
	private DatabaseConnection connection;
	private Expression<String> statement;

	public LoadIntoStatementTask(
			Prologue prologue,
			PageIdentifierExpression page,
			ColumnExpressions columns,
			DatabaseConnection connection,
			Expression<String> statement) {

		super(prologue);
		this.page = page;
		this.columns = columns;
		this.connection = connection;
		this.statement = statement;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, false);
		String statement = this.statement.evaluate();

		try {
			ReadPage readPage = page.load(context.files);
			Columns columns = this.columns.evaluate(readPage.getReadHeaders());
			readIntoStatement(context, connection, readPage, columns, statement);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred loading " + page.getName() + ": " + message);
		}
	}
}
