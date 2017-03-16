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

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;

public class DoTask extends Task implements TaskSetParent {

	private Expression<Boolean> whileCondition;
	private NestedTaskSet taskSet;

	public DoTask(
			Prologue prologue,
			Expression<Boolean> whileCondition) {

		super(prologue);

		this.whileCondition = whileCondition;
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

		Context nestedContext = context.makeNestedContext(getName());

		try {
			if (whileCondition == null) {
				taskSet.run(nestedContext);
			}
			else {
				while (whileCondition.evaluate()) {
					taskSet.runForRerun(nestedContext);
				}
			}
		}
		finally {
			nestedContext.close();
		}
	}

}
