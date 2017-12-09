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

import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.process.Context;

public class CreateTask extends FileTask {

	private PageIdentifierExpression page;
	private PageOptions options;
	private TargetHeaderExpressions headers;

	public CreateTask(
			Prologue prologue,
			PageIdentifierExpression page,
			PageOptions options,
			TargetHeaderExpressions headers) {

		super(prologue);
		this.page = page;
		this.options = options;
		this.headers = headers;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context, true);
		TargetHeaders headers = this.headers.evaluate();

		try {
			page.create(context.files, options, headers);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred creating " + page.getName() + ": " + message);
		}
	}
}
