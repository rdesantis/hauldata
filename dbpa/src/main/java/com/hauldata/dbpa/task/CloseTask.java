/*
 * Copyright (c) 2017, Ronald DeSantis
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

import java.nio.file.Path;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class CloseTask extends Task {

	private Expression<String> file;

	public CloseTask(
			Prologue prologue,
			Expression<String> file) {

		super(prologue);
		this.file = file;
	}

	@Override
	protected void execute(Context context) throws Exception {

		Path path = context.getWritePath(file.evaluate());
		boolean wasOpen = context.files.assureNotOpen(path);
		if (!wasOpen) {
			throw new RuntimeException("File is not open");
		}
	}
}
