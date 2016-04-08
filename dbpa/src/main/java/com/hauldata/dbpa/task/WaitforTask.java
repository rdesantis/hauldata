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

public abstract class WaitforTask extends Task {

	private Expression<String> delayOrTime;

	public WaitforTask(
			Prologue prologue,
			Expression<String> delayOrTime) {

		super(prologue);
		this.delayOrTime = delayOrTime;
	}

	@Override
	protected void execute(Context context) throws Exception {

		String delayOrTime = this.delayOrTime.evaluate();
		long millis = sleepMillis(delayOrTime);

		try {
			boolean longSleep = context.prepareToSleep(millis);

			Thread.sleep(millis);

			context.wakeFromSleep(longSleep);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Wait terminated due to interruption");
		}
	}

	protected abstract long sleepMillis(String delayOrTime);
}
