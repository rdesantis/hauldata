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
import java.util.ArrayList;

import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.ReadHeaders;
import com.hauldata.dbpa.file.ReadPage;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.VariableBase;
import com.hauldata.util.tokenizer.EndOfLine;

public class ForReadTask extends FileTask implements TaskSetParent {

	private ArrayList<VariableBase> variables;
	private PageIdentifierExpression page;
	private ReadHeaderExpressions headers;
	private ColumnExpressions columns;
	private NestedTaskSet taskSet;

	public ForReadTask(
			Task.Prologue prologue,
			ArrayList<VariableBase> variables,
			PageIdentifierExpression page,
			ReadHeaderExpressions headers,
			ColumnExpressions columns) {

		super(prologue);
		this.variables = variables;
		this.page = page;
		this.headers = headers;
		this.columns = columns;
	}

	@Override
	public Task setTaskSet(NestedTaskSet taskSet) {
		this.taskSet = taskSet;
		return this;
	}

	@Override
	public NestedTaskSet getTaskSet() {
		return taskSet;
	}

	@Override
	protected void execute(Context context) throws Exception {

		PageIdentifier page = this.page.evaluate(context, false);
		ReadHeaders headers = this.headers.evaluate();

		context.files.assureNotOpen(page.getPath());
		Context nestedContext = null;
		try {
			ReadPage readPage = page.read(context.files, headers);
			Columns columns = this.columns.evaluate(readPage.getReadHeaders());

			nestedContext = context.makeNestedContext(getName());

			while (readRowIntoVariables(readPage, columns, variables)) {
				taskSet.runForRerun(nestedContext);
			}
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading " + page.getName() + ": " + message);
		}
		finally {
			if (nestedContext != null) {
				nestedContext.close();
			}
		}
	}

	private boolean readRowIntoVariables(ReadPage page, Columns columns, ArrayList<VariableBase> variables) throws IOException, InterruptedException {

		if (!page.hasRow()) {
			return false;
		}

		int maxVariableIndex = 0;

		int columnIndex = 1;
		for (Object value = null; (value = page.readColumn(columnIndex)) != EndOfLine.value; ++columnIndex) {

			int[] variablesIndexes = columns.getTargetColumnIndexes(columnIndex);
			for (int variableIndex : variablesIndexes) {

				if (variableIndex > variables.size()) {
					throw new RuntimeException("Retrieved more columns than variables to update");
				}
				variables.get(variableIndex - 1).setValueChecked(value);

				if (maxVariableIndex < variableIndex) {
					maxVariableIndex = variableIndex;
				}
			}
		}

		if (maxVariableIndex < variables.size()) {
			throw new RuntimeException("Retrieved fewer columns than variables to update");
		}
		
		return true;
	}
}
