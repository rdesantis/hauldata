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

import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.ReadHeaders;
import com.hauldata.dbpa.file.ReadPage;
import com.hauldata.dbpa.process.Context;

public class ReadIntoTokenizedStatementTask extends FileTask {

	private PageIdentifierExpression page;
	private ReadHeaderExpressions headers;
	private ColumnExpressions columns;
	private String statement;

	public ReadIntoTokenizedStatementTask(
			Prologue prologue,
			PageIdentifierExpression page,
			ReadHeaderExpressions headers,
			ColumnExpressions columns,
			String statement) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.columns = columns;
		this.statement = statement;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, false);
		ReadHeaders headers = this.headers.evaluate();

		context.files.assureNotOpen(page.getPath());
		try {
			ReadPage readPage = page.read(context.files, headers);
			Columns columns = this.columns.evaluate(readPage.getReadHeaders());
			readIntoStatement(context, readPage, columns, statement);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
	}
}
