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
import com.hauldata.dbpa.variable.Values;
import com.hauldata.dbpa.variable.Variable;

public class TruncateTask extends Task {
	private Variable<Values> variable;

	public TruncateTask(
			Prologue prologue,
			Variable<Values> variable) {
		super(prologue);
		this.variable = variable;
	}

	@Override
	protected void execute(Context context) throws Exception {
		variable.getValue().clear();
	}
}
