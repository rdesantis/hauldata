/*
 * Copyright (c) 2018, Ronald DeSantis
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

public class BreakTask extends LoggingTask {

	public static final String breakingMessage = "Breaking";

	private Expression<String> message;

	public BreakTask(
			Prologue prologue,
			Expression<String> message) {
		super(prologue);
		this.message = message;
	}

	@Override
	protected void execute(Context context) {

		String messageValue = (message != null) ? message.evaluate() : null;
		if (messageValue != null) {
			context.logger.message(getName(), messageValue);
		}
		else {
			context.logger.info(getName(), breakingMessage);
		}
		throw new BreakingException();
	}
}
