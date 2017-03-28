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

import java.util.Map;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.NestedTaskSet;
import com.hauldata.util.schedule.ScheduleSet;

public abstract class ScheduleTask extends Task implements TaskSetParent {

	private NestedTaskSet taskSet;
	private Map<String, Connection> connections;

	public ScheduleTask(
			Prologue prologue,
			Map<String, Connection> connections) {

		super(prologue);
		this.connections = connections;
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

	protected void execute(Context context, ScheduleSet schedules) throws Exception {
		
		Context nestedContext = context.makeNestedContext(getName());

		try {
			if (schedules.isImmediate()) {
				taskSet.run(nestedContext);
			}

			long sleepMillis;
			while ( 0 < (sleepMillis = schedules.untilNext())) {

				boolean longSleep = context.prepareToSleep(sleepMillis, connections);

				schedules.sleepUntilNext();

				context.wakeFromSleep(longSleep, connections);

				taskSet.run(nestedContext);
			}
		}
		finally {
			nestedContext.close();
		}
	}
}
