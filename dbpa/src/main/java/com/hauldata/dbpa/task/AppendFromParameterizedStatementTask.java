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
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.hauldata.dbpa.expression.ExpressionBase;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.WritePage;
import com.hauldata.dbpa.process.Context;

public class AppendFromParameterizedStatementTask extends FileTask {

	private PageIdentifierExpression page;
	private List<ExpressionBase> expressions;
	private String statement;

	public AppendFromParameterizedStatementTask(
			Prologue prologue,
			PageIdentifierExpression page,
			List<ExpressionBase> expressions,
			String statement) {

		super(prologue);
		this.page = page;
		this.expressions = expressions;
		this.statement = statement;
	}

	@Override
	protected void execute(Context context) {

		PageIdentifier page = this.page.evaluate(context);
		List<Object> values = this.expressions.stream().map(e -> e.getEvaluationObject()).collect(Collectors.toCollection(LinkedList::new));

		try {
			WritePage writePage = page.append(context.files);
			writeFromParameterizedStatement(context, writePage, values, statement);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred appending " + page.getName() + ": " + message);
		}
	}
}
