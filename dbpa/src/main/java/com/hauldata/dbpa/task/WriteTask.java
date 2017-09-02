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

import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.file.FileOptions;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.file.TargetPage;
import com.hauldata.dbpa.process.Context;

public class WriteTask extends FileTask {

	private PageIdentifierExpression page;
	private FileOptions options;
	private TargetHeaderExpressions headers;
	private Source source;

	public WriteTask(
			Prologue prologue,
			PageIdentifierExpression page,
			FileOptions options,
			TargetHeaderExpressions headers,
			Source source) {

		super(prologue);
		this.page = page;
		this.options = options;
		this.headers = headers;
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, true);
		TargetHeaders headers = this.headers.evaluate();

		try {
			TargetPage targetPage = page.write(context.files, options, headers);
			write(context, source, targetPage);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred writing " + page.getName() + ": " + message);
		}
	}
}
