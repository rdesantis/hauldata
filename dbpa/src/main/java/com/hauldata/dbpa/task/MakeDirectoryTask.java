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

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class MakeDirectoryTask extends Task {

	public MakeDirectoryTask(
			Prologue prologue,
			Expression<String> path) {

		super(prologue);
		this.path = path;
	}

	@Override
	protected void execute(Context context) {

		Path path = context.getDataPath(this.path.evaluate());
		try {
			Files.createDirectory(path);
		}
		catch (FileAlreadyExistsException ex)
		{
			throw new RuntimeException("Directory " + path.toString() + " already exists; cannot create");
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to create directory " + path.toString() + " - " + message, ex);
		}
	}

	private Expression<String> path;
}
