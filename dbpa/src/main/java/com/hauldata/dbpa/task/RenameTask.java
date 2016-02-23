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
import java.nio.file.Files;
import java.nio.file.Path;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class RenameTask extends RenameOrCopyTask {

	public RenameTask(
			Prologue prologue,
			Expression<String> from,
			Expression<String> to) {

		super(prologue);
		this.from = from;
		this.to = to;
	}

	@Override
	protected void execute(Context context) {
		doAction(context, context.getDataPath(from.evaluate()), context.getDataPath(to.evaluate()));
	}

	@Override
	protected String verb() {
		return "rename";
	}

	@Override
	protected void action(Path from, Path to) throws IOException {
		Files.move(from, to);
	}

	private Expression<String> from;
	private Expression<String> to;
}
