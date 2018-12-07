/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.SourcePage;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.task.expression.ColumnExpressions;
import com.hauldata.dbpa.task.expression.PageIdentifierExpression;

public class LoadTask extends FileTask {

	private PageIdentifierExpression page;
	private ColumnExpressions columns;
	private DataTarget target;

	public LoadTask(
			Prologue prologue,
			PageIdentifierExpression page,
			ColumnExpressions columns,
			DataTarget target) {

		super(prologue);
		this.page = page;
		this.columns = columns;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		PageIdentifier page = this.page.evaluate(context, false);

		try {
			SourcePage sourcePage = page.load(context.files);
			Columns columns = this.columns.evaluate(sourcePage.getReadHeaders());
			read(context, sourcePage, null, columns, target);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
	}
}
