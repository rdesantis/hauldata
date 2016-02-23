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

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.WriteHeaders;
import com.hauldata.dbpa.file.WritePage;
import com.hauldata.dbpa.process.Context;

public class WriteFromTableTask extends FileTask {

	private PageIdentifierExpression page;
	private WriteHeaderExpressions headers;
	private Expression<String> table;

	public WriteFromTableTask(
			Prologue prologue,
			PageIdentifierExpression page,
			WriteHeaderExpressions headers,
			Expression<String> table) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.table = table;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context);
		WriteHeaders headers = this.headers.evaluate();
		String table = this.table.evaluate();

		String statement = "SELECT * FROM " + table;

		try {
			WritePage writePage = page.write(context.files, headers);
			writeFromStatement(context, writePage, statement);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred writing " + page.getName() + ": " + message);
		}
	}
}
