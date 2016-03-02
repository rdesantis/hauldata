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

public class GoTask extends LogTask {

	public GoTask(
			Prologue prologue,
			Expression<String> message) {
		super(prologue, message);
	}

	@Override
	protected void execute(Context context) {
		String goMessage = (message != null) ? message.evaluate() : null; 
		if (goMessage == null) {
			goMessage = "Continuing"; 
		}
		context.logger.message(getName(), goMessage);
	}
}
