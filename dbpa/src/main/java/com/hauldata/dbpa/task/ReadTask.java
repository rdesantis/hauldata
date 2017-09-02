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
import com.hauldata.dbpa.file.FileOptions;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourcePage;
import com.hauldata.dbpa.process.Context;

public class ReadTask extends FileTask {

	private PageIdentifierExpression page;
	private FileOptions options;
	private SourceHeaderExpressions headers;
	private ColumnExpressions columns;
	private DataTarget target;

	public ReadTask(
			Prologue prologue,
			PageIdentifierExpression page,
			FileOptions options,
			SourceHeaderExpressions headers,
			ColumnExpressions columns,
			DataTarget target) {

		super(prologue);
		this.page = page;
		this.options = options;
		this.headers = headers;
		this.columns = columns;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		PageIdentifier page = this.page.evaluate(context, false);
		SourceHeaders headers = this.headers.evaluate();

		context.files.assureNotOpen(page.getPath());
		try {
			SourcePage sourcePage = page.read(context.files, headers);
			Columns columns = this.columns.evaluate(sourcePage.getReadHeaders());
			read(context, sourcePage, headers, columns, target);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
	}
}
