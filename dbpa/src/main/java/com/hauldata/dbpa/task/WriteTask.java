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
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.WriteHeaders;
import com.hauldata.dbpa.file.WritePage;
import com.hauldata.dbpa.process.Context;

public class WriteTask extends FileTask {

	private PageIdentifierExpression page;
	private WriteHeaderExpressions headers;
	private Source source;

	public WriteTask(
			Prologue prologue,
			PageIdentifierExpression page,
			WriteHeaderExpressions headers,
			Source source) {

		super(prologue);
		this.page = page;
		this.headers = headers;
		this.source = source;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, true);
		WriteHeaders headers = this.headers.evaluate();

		try {
			WritePage writePage = page.write(context.files, headers);
			write(context, source, writePage);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred writing " + page.getName() + ": " + message);
		}
	}
}
