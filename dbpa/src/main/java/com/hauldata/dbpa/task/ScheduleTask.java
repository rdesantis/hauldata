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

public abstract class ScheduleTask extends Task {

	private NestedTaskSet taskSet;
	private Map<String, Connection> connections;

	public ScheduleTask(
			Prologue prologue,
			NestedTaskSet taskSet,
			Map<String, Connection> connections) {

		super(prologue);
		this.taskSet = taskSet;
		this.connections = connections;
	}

	protected void execute(Context context, ScheduleSet schedules) {
		
		Context nestedContext = context.makeNestedContext(getName());

		try {
			if (schedules.isImmediate()) {
				taskSet.runForRerun(nestedContext);
			}

			long sleepMillis;
			while ( 0 < (sleepMillis = schedules.untilNext())) {

				boolean longSleep = context.prepareToSleep(sleepMillis, connections);

				schedules.sleepUntilNext();

				context.wakeFromSleep(longSleep, connections);

				taskSet.runForRerun(nestedContext);
			}
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Schedule terminated due to interruption");
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to run nested tasks - " + message);
		}
		finally {
			nestedContext.close();
		}
	}
}
