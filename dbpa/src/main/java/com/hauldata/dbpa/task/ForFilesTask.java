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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.PatternSyntaxException;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.dbpa.variable.Variable;

public class ForFilesTask extends Task implements TaskSetParent {

	private Variable<String> variable;
	private Expression<String> filename;
	private NestedTaskSet taskSet;

	public ForFilesTask(
			Task.Prologue prologue,
			Variable<String> variable,
			Expression<String> filename) {

		super(prologue);
		this.variable = variable;
		this.filename = filename;
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

		Context nestedContext = null;
		String sourceName = null;
		DirectoryStream<Path> sourcePaths = null;
		try {
			nestedContext = context.makeNestedContext(getName());

			sourceName = filename.evaluate();
			String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(sourceName);
			Path parentPath = context.getReadPath(parentAndFileName[0]);

			sourcePaths = Files.newDirectoryStream(parentPath, parentAndFileName[1]);
			
			for (Path sourcePath : sourcePaths) {
				if (Files.isRegularFile(sourcePath)) {

					variable.setValueObject(sourcePath.getFileName().toString());
					taskSet.runForRerun(nestedContext);
				}
			}
		}
		catch (PatternSyntaxException | /* NotDirectoryException | */ IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred resolving FILE " + sourceName + " - " + message, ex);
		}
		finally {
			if (sourcePaths != null) {
				try { sourcePaths.close(); } catch (Exception ex) {}
			}
			if (nestedContext != null) { nestedContext.close(); }
		}
	}
}
