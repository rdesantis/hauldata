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
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.schedule.ScheduleSet;

public class OnScheduleTask extends ScheduleTask {

	private Expression<String> schedule;

	public OnScheduleTask(
			Prologue prologue,
			Expression<String> schedule,
			Map<String, Connection> connections) {

		super(prologue, connections);
		this.schedule = schedule;
	}

	@Override
	protected void execute(Context context) {

		ScheduleSet schedules;
		try {
			schedules = ScheduleSet.parse(schedule.evaluate());
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error parsing schedule - " + message);
		}

		execute(context, schedules);
	}
}
