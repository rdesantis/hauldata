/*
 * Copyright (c) 2020, Ronald DeSantis
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

import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.variable.Table;
import com.hauldata.dbpa.variable.Variable;

public class TruncateTask extends Task {
	private Variable<Table> variable;

	public TruncateTask(
			Prologue prologue,
			Variable<Table> variable) {
		super(prologue);
		this.variable = variable;
	}

	@Override
	protected void execute(Context context) throws Exception {
		// Rather than clearing the existing list, allocate a new value with a new list
		// so that any asynchronous child process or concurrent task that is accessing the previous list
		// can continue to access it.
		variable.setValue(new Table());
	}
}
